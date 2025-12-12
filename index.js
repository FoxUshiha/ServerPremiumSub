// index.js — Bot premium por card (single-file, com verificação forte de pagamentos)
// Dependências: discord.js v14, sqlite3, axios
import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { Client, GatewayIntentBits, Partials, REST, Routes, SlashCommandBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle, ModalBuilder, TextInputBuilder, TextInputStyle, EmbedBuilder, PermissionsBitField } from 'discord.js';
import axios from 'axios';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

// ---- Config / env ----
const {
  DISCORD_TOKEN = '',
  COIN_API_URL = 'https://bank.foxsrv.net/',
  SERVER_RECEIVER_CARD = '',
  DEFAULT_GUILD_PRICE = '0.00001000',
  DB_PATH = './database.db',
  CLIENT_ID = '',
  ACTIVATION_MS = String(30 * 24 * 3600 * 1000), // default 30 days in ms
  CHECK_INTERVAL_MS = String(5 * 60 * 1000) // default 10 minutes in ms
} = process.env;

if (!DISCORD_TOKEN) {
  console.error('DISCORD_TOKEN missing in env');
  process.exit(1);
}
if (!SERVER_RECEIVER_CARD) {
  console.error('SERVER_RECEIVER_CARD missing in env — set the card that will receive guild payments');
  process.exit(1);
}

const activationMsNum = Number(ACTIVATION_MS) || (30 * 24 * 3600 * 1000);
const activationSec = Math.max(1, Math.floor(activationMsNum / 1000));
const checkIntervalMs = Math.max(1000, Number(CHECK_INTERVAL_MS) || (5 * 60 * 1000));

// ---- DB setup (sqlite) ----
let db;
async function initDb() {
  await fs.promises.mkdir(path.dirname(DB_PATH), { recursive: true }).catch(()=>{});
  db = await open({ filename: DB_PATH, driver: sqlite3.Database });
  await db.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS guilds (
      guild_id TEXT PRIMARY KEY,
      log_channel_id TEXT,
      server_card TEXT,
      price TEXT,
      role_id TEXT,
      last_guild_payment_ts INTEGER DEFAULT 0,
      active INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS subscriptions (
      guild_id TEXT,
      user_id TEXT,
      card_code TEXT,
      subscribed_ts INTEGER,
      last_renew_ts INTEGER,
      active INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id, user_id),
      FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS ix_subs_guild_active ON subscriptions(guild_id, active);

    CREATE TABLE IF NOT EXISTS payments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT,
      user_id TEXT,
      from_card TEXT,
      to_card TEXT,
      amount TEXT,
      success INTEGER,
      txid TEXT,
      raw TEXT,
      ts INTEGER
    );
  `);
}

// ---- Discord client ----
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel]
});

// ---- Helper: coin API calls ----
// Normalize COIN_API_URL so baseURL always ends with '/api' (no double slashes)
const normalizedApiBase = (() => {
  try {
    const raw = String(COIN_API_URL || '').trim();
    if (!raw) return '';
    const noTrail = raw.replace(/\/+$/, '');
    if (/\/api$/i.test(noTrail)) return noTrail;
    return noTrail + '/api';
  } catch (e) {
    return COIN_API_URL;
  }
})();

const coin = axios.create({
  baseURL: normalizedApiBase,
  timeout: 15000,
  headers: { 'Content-Type': 'application/json' }
});

// utility: safe JSON stringify
function safeJson(v) {
  try { return JSON.stringify(v); } catch (e) { return String(v); }
}

// call /api/card/pay
async function callCardPay(fromCard, toCard, amount) {
  const truncated = Math.floor(Number(amount) * 1e8) / 1e8;
  try {
    const res = await coin.post('/card/pay', { fromCard, toCard, amount: truncated });
    return res.data;
  } catch (err) {
    // If server returned HTML page or text, err.response.data might be string.
    if (err.response && err.response.data) return err.response.data;
    return { success: false, error: err.message || 'request_error' };
  }
}

// create bill + pay flow
async function callBillCreateAndPay(fromCard, toCard, amount) {
  const truncated = Math.floor(Number(amount) * 1e8) / 1e8;
  try {
    const create = await coin.post('/bill/create/card', { fromCard, toCard, amount: truncated, time: Date.now() });
    if (!create.data || !create.data.billId) return { success: false, error: 'create_failed', raw: create.data };
    const pay = await coin.post('/bill/pay/card', { cardCode: fromCard, billId: create.data.billId });
    return pay.data || { success: true, raw: pay.data };
  } catch (err) {
    if (err.response && err.response.data) return err.response.data;
    return { success: false, error: err.message || 'request_error' };
  }
}

// Try to verify a txid by hitting likely endpoints. Returns true only if verification indicates success/confirmed.
async function verifyTxOnApi(txid) {
  if (!txid) return false;
  const endpoints = [
    `/tx/${txid}`,
    `/transaction/${txid}`,
    `/transactions/${txid}`,
    `/txs/${txid}`
  ];
  for (const ep of endpoints) {
    try {
      const res = await coin.get(ep).catch(()=>null);
      if (!res || !res.data) continue;
      const data = res.data;
      // if data explicitly success true or status indicates confirmed/success -> accept
      if (data && (data.success === true || data.status === 'confirmed' || data.status === 'success' || data.state === 'confirmed' || data.confirmed === true)) return true;
      // some APIs return { tx: { status: 'confirmed' } }
      const json = safeJson(data).toLowerCase();
      if (json.includes('confirmed') || json.includes('success')) return true;
    } catch (e) {
      // ignore and try next
    }
  }
  return false;
}

// Strong verification: determine if API response indicates success
function responseLooksLikeHtml(obj) {
  return (typeof obj === 'string' && obj.trim().toLowerCase().startsWith('<!doctype')) ||
         (typeof obj === 'string' && obj.trim().startsWith('<html'));
}

async function attemptCharge(fromCard, toCard, amountStr, meta = {}) {
  // meta optional { guildId, userId } for logging
  const now = nowTs();
  // 1) direct /card/pay
  const r = await callCardPay(fromCard, toCard, amountStr);

  // record attempt provisional data (we'll write full payment log after verification)
  let finalSuccess = false;
  let finalTx = null;
  let rawForLog = r;

  // if response is string HTML -> failure
  if (responseLooksLikeHtml(r)) {
    finalSuccess = false;
  } else {
    // if object
    if (r && typeof r === 'object') {
      if (r.success === true && !r.error) {
        // explicit success — but double-check tx presence if possible
        const txid = (r.txId || r.tx_id || r.txid || r.tx);
        if (txid) {
          const verified = await verifyTxOnApi(txid).catch(()=>false);
          if (verified) { finalSuccess = true; finalTx = txid; }
          else {
            // if verify failed but API explicitly returned success true and no error, accept (backwards compat)
            finalSuccess = true;
            finalTx = txid || null;
          }
        } else {
          // success true without txid -> still accept
          finalSuccess = true;
        }
      } else {
        // no explicit success; if txId exists and no error, try to verify by querying tx endpoint
        const txid = (r.txId || r.tx_id || r.txid || r.tx);
        if (txid && !r.error) {
          const verified = await verifyTxOnApi(txid).catch(()=>false);
          if (verified) { finalSuccess = true; finalTx = txid; }
        }
      }
    }
  }

  // if still not success, try fallback bill create+pay
  let b = null;
  if (!finalSuccess) {
    b = await callBillCreateAndPay(fromCard, toCard, amountStr);
    rawForLog = rawForLog || b;
    if (responseLooksLikeHtml(b)) {
      finalSuccess = false;
    } else if (b && typeof b === 'object') {
      if (b.success === true && !b.error) {
        const txid = (b.txId || b.tx_id || b.txid || b.tx);
        if (txid) {
          const verified = await verifyTxOnApi(txid).catch(()=>false);
          if (verified) { finalSuccess = true; finalTx = txid; }
          else {
            finalSuccess = true;
            finalTx = txid || null;
          }
        } else {
          finalSuccess = true;
        }
      } else {
        const txid = (b.txId || b.tx_id || b.txid || b.tx);
        if (txid && !b.error) {
          const verified = await verifyTxOnApi(txid).catch(()=>false);
          if (verified) { finalSuccess = true; finalTx = txid; }
        }
      }
    }
  }

  // Finally: log attempt into DB payments table for audit
  try {
    const rawStr = safeJson({ direct: r, bill: b });
    await db.run('INSERT INTO payments (guild_id, user_id, from_card, to_card, amount, success, txid, raw, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [meta.guildId || null, meta.userId || null, String(fromCard), String(toCard), String(amountStr), finalSuccess ? 1 : 0, finalTx || null, rawStr, now]);
  } catch (e) {
    console.warn('Failed to log payment attempt', e);
  }

  return { success: finalSuccess, raw: { direct: r, bill: b }, txid: finalTx };
}

// ---- Utilities ----
function formatCoin(numStr) {
  const n = Number(numStr || '0');
  if (Number.isNaN(n)) return '0.00000000';
  return n.toFixed(8);
}
function nowTs() { return Math.floor(Date.now() / 1000); }
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

// ---- Commands definitions (unchanged) ----
const commands = [
  new SlashCommandBuilder().setName('log').setDescription('Configura o canal de logs da guild').addChannelOption(opt => opt.setName('channel').setDescription('Canal para receber logs').setRequired(true)),
  new SlashCommandBuilder().setName('card').setDescription('Registrar/atualizar seu card para assinaturas nesta guild'),
  new SlashCommandBuilder().setName('channel').setDescription('Enviar embed com botão subscribe no canal selecionado').addChannelOption(opt=>opt.setName('channel').setDescription('Canal para postar o painel').setRequired(true)),
  new SlashCommandBuilder().setName('worth').setDescription('Define o preço de assinatura da guild (8 casas decimais)').addStringOption(o=>o.setName('price').setDescription('ex: 0.05000000').setRequired(true)),
  new SlashCommandBuilder().setName('servercard').setDescription('Administrador: define o card do servidor (ativa o sistema)').addStringOption(o=>o.setName('card').setDescription('card do servidor').setRequired(true)),
  new SlashCommandBuilder().setName('role').setDescription('Seleciona a role que será dada a assinantes').addRoleOption(o=>o.setName('role').setDescription('Role a ser aplicada').setRequired(true))
].map(c => c.toJSON());

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);
  try {
    if (CLIENT_ID) {
      await rest.put(Routes.applicationCommands(CLIENT_ID), { body: commands });
      console.log('Registered global commands.');
    } else {
      console.log('CLIENT_ID not provided: commands may be registered on startup via client.application.id flow.');
    }
  } catch (err) {
    console.warn('Failed registering commands:', err && err.stack ? err.stack : err);
  }
}

// ---- On ready ----
client.once('ready', async () => {
  console.log('Logged in as', client.user.tag);
  await initDb();
  await registerCommands();

  // ensure every guild row exists; mark inactive when missing server_card
  const guildsToProcess = Array.from(client.guilds.cache.keys());
  const now = nowTs();
  for (const guildId of guildsToProcess) {
    const g = await db.get('SELECT guild_id, server_card FROM guilds WHERE guild_id = ?', guildId);
    if (!g) {
      const pseudoOldTs = now - activationSec;
      await db.run('INSERT INTO guilds (guild_id, server_card, price, last_guild_payment_ts, active) VALUES (?, ?, ?, ?, ?)', [guildId, null, DEFAULT_GUILD_PRICE, pseudoOldTs, 0]);
    } else {
      if (!g.server_card) {
        const pseudoOldTs = now - activationSec;
        await db.run('UPDATE guilds SET last_guild_payment_ts = ?, active = 0 WHERE guild_id = ?', [pseudoOldTs, guildId]);
      }
    }
  }

  // periodicCheckout interval configurable
  setInterval(periodicCheckout, checkIntervalMs);
  setTimeout(periodicCheckout, 5000);
});

// When bot joins a new guild
client.on('guildCreate', async (guild) => {
  try {
    const now = nowTs();
    const g = await db.get('SELECT guild_id, server_card FROM guilds WHERE guild_id = ?', guild.id);
    if (!g) {
      const pseudoOldTs = now - activationSec;
      await db.run('INSERT INTO guilds (guild_id, server_card, price, last_guild_payment_ts, active) VALUES (?, ?, ?, ?, ?)', [guild.id, null, DEFAULT_GUILD_PRICE, pseudoOldTs, 0]);
    } else if (!g.server_card) {
      const pseudoOldTs = now - activationSec;
      await db.run('UPDATE guilds SET last_guild_payment_ts = ?, active = 0 WHERE guild_id = ?', [pseudoOldTs, guild.id]);
    }
  } catch (e) {
    console.warn('guildCreate handler error', e);
  }
});

async function getGuildRow(guildId) {
  const row = await db.get('SELECT * FROM guilds WHERE guild_id = ?', guildId);
  return row;
}

// ---- Interactions (commands / buttons / modal) ----
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      const { commandName } = interaction;

      const adminCommands = new Set(['log','servercard','worth','role']);

      if (!adminCommands.has(commandName)) {
        const guildRow = await getGuildRow(interaction.guildId);
        if (!guildRow || Number(guildRow.active) === 0) {
          return interaction.reply({ content: 'Este servidor está inativo (sem card configurado). Apenas administradores podem configurar o sistema com /servercard. Até a guild ser ativada, comandos premium estão bloqueados.', ephemeral: true });
        }
      }

      if (commandName === 'log') {
        if (!interaction.member.permissions.has(PermissionsBitField.Flags.Administrator)) return interaction.reply({ content: 'Somente administradores podem configurar o canal de logs.', ephemeral: true });
        const channel = interaction.options.getChannel('channel');
        await db.run('INSERT OR REPLACE INTO guilds (guild_id, log_channel_id, server_card, price, role_id, active) VALUES (?, COALESCE((SELECT log_channel_id FROM guilds WHERE guild_id = ?), ?), COALESCE((SELECT server_card FROM guilds WHERE guild_id = ?), ?), COALESCE((SELECT price FROM guilds WHERE guild_id = ?), ?), COALESCE((SELECT role_id FROM guilds WHERE guild_id = ?), ?), COALESCE((SELECT active FROM guilds WHERE guild_id = ?), 1))',
          [interaction.guildId, interaction.guildId, channel.id, interaction.guildId, null, interaction.guildId, DEFAULT_GUILD_PRICE, interaction.guildId, null, interaction.guildId]);
        await interaction.reply({ content: `Canal de logs configurado: ${channel}`, ephemeral: true });
        return;
      }

      if (commandName === 'servercard') {
        if (!interaction.member.permissions.has(PermissionsBitField.Flags.Administrator)) return interaction.reply({ content: 'Somente administradores podem definir o card do servidor.', ephemeral: true });
        const card = interaction.options.getString('card');
        await db.run('INSERT INTO guilds (guild_id, server_card, price, active, last_guild_payment_ts) VALUES (?, ?, COALESCE((SELECT price FROM guilds WHERE guild_id = ?), ?), 1, 0) ON CONFLICT(guild_id) DO UPDATE SET server_card = excluded.server_card, active = 1, last_guild_payment_ts = 0', [interaction.guildId, card, interaction.guildId, DEFAULT_GUILD_PRICE]);
        await interaction.reply({ content: `Card do servidor atualizado. Sistema ativado nesta guild (o bot tentará cobrar em seguida).`, ephemeral: true });
        return;
      }

      if (commandName === 'worth') {
        if (!interaction.member.permissions.has(PermissionsBitField.Flags.Administrator)) return interaction.reply({ content: 'Somente administradores podem definir o preço.', ephemeral: true });
        const price = interaction.options.getString('price');
        if (!/^\d+(\.\d{1,8})?$/.test(price)) return interaction.reply({ content: 'Formato inválido. Use até 8 casas decimais, ex: 0.05000000', ephemeral: true });
        await db.run('INSERT INTO guilds (guild_id, price) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET price = excluded.price', [interaction.guildId, price]);
        await interaction.reply({ content: `Preço de inscrição definido para ${formatCoin(price)} coins (ciclo configurável via env).`, ephemeral: true });
        return;
      }

      if (commandName === 'role') {
        if (!interaction.member.permissions.has(PermissionsBitField.Flags.Administrator)) return interaction.reply({ content: 'Somente administradores podem definir a role.', ephemeral: true });
        const role = interaction.options.getRole('role');
        await db.run('INSERT INTO guilds (guild_id, role_id) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET role_id = excluded.role_id', [interaction.guildId, role.id]);
        await interaction.reply({ content: `Role de assinante definida: ${role.name}`, ephemeral: true });
        return;
      }

      if (commandName === 'card') {
        const modal = new ModalBuilder()
          .setCustomId(`card_modal::${interaction.guildId}`)
          .setTitle('Registrar seu Card');
        const input = new TextInputBuilder()
          .setCustomId('card_input')
          .setLabel('Coloque seu Card ID (ex: abc123)')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(128);
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (commandName === 'channel') {
        if (!interaction.member.permissions.has(PermissionsBitField.Flags.ManageChannels)) return interaction.reply({ content: 'Você precisa de permissão de Gerenciar Canais para postar o painel.', ephemeral: true });
        const channel = interaction.options.getChannel('channel');
        const guildRow = await getGuildRow(interaction.guildId) || {};
        const price = formatCoin(guildRow.price || DEFAULT_GUILD_PRICE);
        const roleMention = guildRow.role_id ? `<@&${guildRow.role_id}>` : '*nenhuma role configurada*';
        const embed = new EmbedBuilder()
          .setTitle('Painel de Assinatura Premium')
          .setDescription(`Assine o serviço premium — ${price} coins a cada ciclo configurado.\nRole concedida: ${roleMention}`)
          .setFooter({ text: 'Clique em Subscribe para se inscrever' });
        const btn = new ButtonBuilder().setCustomId(`subscribe::${interaction.guildId}`).setLabel('Subscribe').setStyle(ButtonStyle.Primary);
        await channel.send({ embeds: [embed], components: [new ActionRowBuilder().addComponents(btn)] });
        await interaction.reply({ content: `Painel postado em ${channel}`, ephemeral: true });
        return;
      }

    } else if (interaction.isModalSubmit()) {
      if (interaction.customId && interaction.customId.startsWith('card_modal::')) {
        const guildId = interaction.customId.split('::')[1];
        const guildRow = await getGuildRow(guildId);
        if (!guildRow || Number(guildRow.active) === 0) {
          return interaction.reply({ content: 'Este servidor está inativo (sem card do servidor configurado). Administradores precisam usar /servercard para ativar o sistema.', ephemeral: true });
        }

        const cardInput = interaction.fields.getTextInputValue('card_input').trim();
        const row = guildRow || {};
        const price = row.price || DEFAULT_GUILD_PRICE;
        const ts = nowTs();
        await db.run('INSERT INTO subscriptions (guild_id, user_id, card_code, subscribed_ts, last_renew_ts, active) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(guild_id,user_id) DO UPDATE SET card_code = excluded.card_code, subscribed_ts = excluded.subscribed_ts', [guildId, interaction.user.id, cardInput, ts, ts, 0]);
        await interaction.reply({ content: 'Card recebido — tentando efetuar o pagamento inicial...', ephemeral: true });

        const serverCard = (row && row.server_card) ? row.server_card : SERVER_RECEIVER_CARD;
        const amount = formatCoin(row && row.price ? row.price : price);

        // Pass meta to attemptCharge so it logs guild/user in payments table
        const attempt = await attemptCharge(cardInput, serverCard, amount, { guildId, userId: interaction.user.id });
        const logChanId = (row && row.log_channel_id) ? row.log_channel_id : null;
        const logChannel = logChanId ? await client.channels.fetch(logChanId).catch(()=>null) : null;

        if (attempt.success) {
          // only mark active after explicit verified success
          await db.run('UPDATE subscriptions SET active = 1, last_renew_ts = ? WHERE guild_id = ? AND user_id = ?', [nowTs(), guildId, interaction.user.id]);
          if (row && row.role_id) {
            try {
              const guildObj = await client.guilds.fetch(guildId);
              const member = await guildObj.members.fetch(interaction.user.id).catch(()=>null);
              if (member) await member.roles.add(row.role_id).catch(()=>null);
            } catch (e) {}
          }
          if (logChannel && logChannel.isTextBased()) {
            logChannel.send({ embeds: [new EmbedBuilder().setTitle('Pagamento de Assinatura — Sucesso').setDescription(`<@${interaction.user.id}> pagou ${amount} coins. TX: ${attempt.txid || 'n/a'}`).setTimestamp()] }).catch(()=>null);
          }
          await interaction.followUp({ content: 'Pagamento inicial efetuado com sucesso — você está ativo!', ephemeral: true });
        } else {
          // explicit failure -> leave inactive
          if (logChannel && logChannel.isTextBased()) {
            const errMsg = attempt.raw ? (attempt.raw.direct?.error || attempt.raw.bill?.error || safeJson(attempt.raw)) : 'unknown';
            logChannel.send({ embeds: [new EmbedBuilder().setTitle('Pagamento de Assinatura — Falha').setDescription(`<@${interaction.user.id}> não pôde pagar ${amount} coins.\nErro: ${errMsg}`).setTimestamp()] }).catch(()=>null);
          }
          await interaction.followUp({ content: `Falha no pagamento inicial. Tente atualizar o card com /card novamente.`, ephemeral: true });
        }
        return;
      }
    } else if (interaction.isButton()) {
      if (!interaction.customId) return;
      if (interaction.customId.startsWith('subscribe::')) {
        const guildId = interaction.customId.split('::')[1];
        const guildRow = await getGuildRow(guildId);
        if (!guildRow || Number(guildRow.active) === 0) {
          return interaction.reply({ content: 'Este servidor está inativo (sem card do servidor configurado). Administradores precisam usar /servercard para ativar o sistema.', ephemeral: true });
        }
        const modal = new ModalBuilder().setCustomId(`card_modal::${guildId}`).setTitle('Registrar seu Card');
        const input = new TextInputBuilder().setCustomId('card_input').setLabel('Coloque seu Card ID').setStyle(TextInputStyle.Short).setRequired(true).setMaxLength(128);
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }
    }
  } catch (err) {
    console.error('interaction error', err);
    try { if (interaction.replied || interaction.deferred) await interaction.followUp({ content: 'Erro interno.', ephemeral: true }); else await interaction.reply({ content: 'Erro interno.', ephemeral: true }); } catch(e){}
  }
});

// ---- Periodic checkout logic ----
async function periodicCheckout() {
  const guilds = await db.all('SELECT * FROM guilds');
  const now = nowTs();
  for (const g of guilds) {
    try {
      // if no server_card, ensure guild is marked inactive and timestamp set to past (activation window passed)
      if (!g.server_card) {
        const pseudoOldTs = now - activationSec;
        await db.run('UPDATE guilds SET last_guild_payment_ts = ?, active = 0 WHERE guild_id = ?', [pseudoOldTs, g.guild_id]);
        continue;
      }

      const price = (g.price || DEFAULT_GUILD_PRICE);
      const lastGuildTs = Number(g.last_guild_payment_ts || 0);
      const nowLocal = now;

      // guild-level payment
      if (lastGuildTs === 0 || (nowLocal - lastGuildTs) >= activationSec) {
        const guildServerCard = g.server_card;
        // include guild meta for payments log
        const attempt = await attemptCharge(guildServerCard, SERVER_RECEIVER_CARD, formatCoin(price), { guildId: g.guild_id });
        const logChan = g.log_channel_id ? await client.channels.fetch(g.log_channel_id).catch(()=>null) : null;
        if (attempt.success) {
          await db.run('UPDATE guilds SET last_guild_payment_ts = ?, active = 1 WHERE guild_id = ?', [nowLocal, g.guild_id]);
          if (logChan && logChan.isTextBased()) logChan.send({ embeds: [new EmbedBuilder().setTitle('Guild Payment').setDescription(`Guild payment succeeded for ${formatCoin(price)} coins. TX: ${attempt.txid || 'n/a'}`).setTimestamp()] }).catch(()=>null);
        } else {
          await db.run('UPDATE guilds SET active = 0 WHERE guild_id = ?', [g.guild_id]);
          if (logChan && logChan.isTextBased()) {
            const errMsg = attempt.raw ? (attempt.raw.direct?.error || attempt.raw.bill?.error || safeJson(attempt.raw)) : 'unknown';
            logChan.send({ embeds: [new EmbedBuilder().setTitle('Guild Payment Failed').setDescription(`Guild payment failed for ${formatCoin(price)} coins. Blocking premium features until fixed.\nErro: ${errMsg}`).setTimestamp()] }).catch(()=>null);
          }
          await removeRoleFromAll(g.guild_id, g.role_id, logChan);
          continue;
        }
      }

      // subscriptions renewal
      const subs = await db.all('SELECT * FROM subscriptions WHERE guild_id = ?', g.guild_id);
      for (const s of subs) {
        try {
          const lastRenew = Number(s.last_renew_ts || s.subscribed_ts || 0);
          if ((nowLocal - lastRenew) >= activationSec) {
            if (!s.card_code) {
              await db.run('UPDATE subscriptions SET active = 0 WHERE guild_id = ? AND user_id = ?', [g.guild_id, s.user_id]);
              await removeRoleFromMember(g.guild_id, s.user_id, g.role_id);
              await notifyUserDMed(s.user_id, `Tentativa de renovar sua assinatura em ${g.guild_id} falhou: card não configurado. Use /card para registrar seu card.`, g.log_channel_id);
              continue;
            }
            const attempt = await attemptCharge(s.card_code, g.server_card || SERVER_RECEIVER_CARD, formatCoin(g.price || DEFAULT_GUILD_PRICE), { guildId: g.guild_id, userId: s.user_id });
            if (attempt.success) {
              await db.run('UPDATE subscriptions SET active = 1, last_renew_ts = ? WHERE guild_id = ? AND user_id = ?', [nowLocal, g.guild_id, s.user_id]);
              await giveRoleToMember(g.guild_id, s.user_id, g.role_id);
              const logChan = g.log_channel_id ? await client.channels.fetch(g.log_channel_id).catch(()=>null) : null;
              if (logChan && logChan.isTextBased()) logChan.send({ embeds: [new EmbedBuilder().setTitle('Subscription Renewed').setDescription(`<@${s.user_id}> renovou a assinatura. TX: ${attempt.txid || 'n/a'}`).setTimestamp()] }).catch(()=>null);
            } else {
              await db.run('UPDATE subscriptions SET active = 0 WHERE guild_id = ? AND user_id = ?', [g.guild_id, s.user_id]);
              await removeRoleFromMember(g.guild_id, s.user_id, g.role_id);
              const errMsg = attempt.raw ? (attempt.raw.direct?.error || attempt.raw.bill?.error || safeJson(attempt.raw)) : 'unknown';
              await notifyUserDMed(s.user_id, `Não foi possível renovar sua assinatura em ${g.guild_id}. Removemos o acesso. Erro: ${errMsg}`, g.log_channel_id);
            }
            await sleep(300);
          }
        } catch (e) {
          console.error('sub renewal error', e);
        }
      }

    } catch (e) {
      console.error('periodic guild error', e);
    }
  }
}

// helper to lock guild features and remove roles
async function lockGuild(guildId, reason) {
  const g = await db.get('SELECT role_id, log_channel_id FROM guilds WHERE guild_id = ?', guildId);
  if (!g) return;
  await db.run('UPDATE guilds SET active = 0 WHERE guild_id = ?', [guildId]);
  await removeRoleFromAll(guildId, g.role_id, client.channels.fetch(g.log_channel_id).catch(()=>null));
}

// remove a role from all members of a guild (best-effort)
async function removeRoleFromAll(guildId, roleId, logChannel) {
  if (!roleId) return;
  try {
    const guildObj = await client.guilds.fetch(guildId);
    await guildObj.members.fetch();
    for (const [, member] of guildObj.members.cache) {
      if (member.roles.cache.has(roleId)) {
        await member.roles.remove(roleId).catch(()=>null);
      }
    }
    if (logChannel && logChannel.isTextBased) logChannel.send({ embeds: [new EmbedBuilder().setTitle('Guild Deactivated').setDescription('Role removed from all members due to guild payment failure.').setTimestamp()] }).catch(()=>null);
  } catch (e) {
    console.warn('removeRoleFromAll failed', e);
  }
}

// remove role from single member
async function removeRoleFromMember(guildId, userId, roleId) {
  if (!roleId) return;
  try {
    const guildObj = await client.guilds.fetch(guildId);
    const member = await guildObj.members.fetch(userId).catch(()=>null);
    if (member && member.roles.cache.has(roleId)) await member.roles.remove(roleId).catch(()=>null);
  } catch (e) {}
}
async function giveRoleToMember(guildId, userId, roleId) {
  if (!roleId) return;
  try {
    const guildObj = await client.guilds.fetch(guildId);
    const member = await guildObj.members.fetch(userId).catch(()=>null);
    if (member && !member.roles.cache.has(roleId)) await member.roles.add(roleId).catch(()=>null);
  } catch (e) {}
}

// DM notify with queue
const dmQueue = [];
let dmRunning = false;
async function notifyUserDMed(userId, message, log_channel_id = null) {
  dmQueue.push({ userId, message, log_channel_id });
  if (!dmRunning) runDmQueue();
}
async function runDmQueue() {
  dmRunning = true;
  while (dmQueue.length) {
    const job = dmQueue.shift();
    try {
      const user = await client.users.fetch(job.userId).catch(()=>null);
      if (user) {
        await user.send({ content: job.message }).catch(()=>null);
      }
      if (job.log_channel_id) {
        const lc = await client.channels.fetch(job.log_channel_id).catch(()=>null);
        if (lc && lc.isTextBased()) lc.send({ embeds: [new EmbedBuilder().setTitle('User Notified').setDescription(`Notificamos <@${job.userId}>: ${job.message}`).setTimestamp()] }).catch(()=>null);
      }
    } catch(e){}
    await sleep(2000);
  }
  dmRunning = false;
}

// ---- login ----
initDb().then(()=>client.login(DISCORD_TOKEN)).catch(err=>{ console.error('DB init/login failed', err); process.exit(1); });

// graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down...');
  try { if (db) await db.close(); } catch (e){}
  client.destroy();
  process.exit(0);
});
