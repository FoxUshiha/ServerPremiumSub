# ServerPremiumSub
Subscribe premium discord role using coins

Depends on:
https://github.com/FoxUshiha/DC-Coin-Bot

You do not need to host or download the coin bot, only setup this bot (but you can host your own coin project if you wish);

Also install NojeJS and use prompt or host to start the project

Install Dependencies:
```
npm install discord.js sqlite3 axios dotenv sqlite
```

Main coin host: https://bank.foxsrv.net/

Discord bot website: https://discord.com/developers/applications

.env file:
```
DISCORD_TOKEN=your_token
CLIENT_ID=ID
COIN_API_URL=https://bank.foxsrv.net/      # exemplo
SERVER_RECEIVER_CARD=YOUR_CARD          # card que receberá pagamentos das guilds
DEFAULT_GUILD_PRICE=0.00001000             # preço padrão por 30 dias (8 casas decimais)
DB_PATH=./database.db
ACTIVATION_MS=2592000000
CHECK_INTERVAL_MS=300000

```

Setup and play!

Thanks for downloading :D
