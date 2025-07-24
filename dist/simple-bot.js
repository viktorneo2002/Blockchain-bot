"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const web3_js_1 = require("@solana/web3.js");
async function main() {
    console.log('🚀 Starting Simple Solana Bot...');
    const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://api.mainnet-beta.solana.com';
    try {
        // Generate a test keypair
        const wallet = web3_js_1.Keypair.generate();
        console.log('✅ Test wallet:', wallet.publicKey.toString());
        // Connect to Solana
        const connection = new web3_js_1.Connection(RPC_ENDPOINT, 'confirmed');
        const version = await connection.getVersion();
        console.log('✅ Connected to Solana:', version);
        // Get recent blockhash
        const blockhash = await connection.getLatestBlockhash();
        console.log('📦 Recent blockhash:', blockhash.blockhash);
        // Check some token accounts
        const USDC_MINT = new web3_js_1.PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
        const supply = await connection.getTokenSupply(USDC_MINT);
        console.log('💵 USDC Supply:', supply.value.uiAmount);
        console.log('\n✅ Bot is working! This is a test implementation.');
    }
    catch (error) {
        console.error('❌ Error:', error);
    }
}
main().catch(console.error);
//# sourceMappingURL=simple-bot.js.map