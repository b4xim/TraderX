# TraderX

A local paper trading dashboard for monitoring an intraday ATM Put options strategy using live market data from Upstox. Each morning, up to three stock candidates are submitted; the app resolves their at-the-money PE instruments, locks in entry prices via live LTP, and streams real-time tick data through Upstox's WebSocket feed — independently tracking each position against a fixed target and stop-loss until the hard exit at 9:32 AM IST. All simulated trades are logged to a local SQLite database with full P&L history. No real orders are ever placed.
