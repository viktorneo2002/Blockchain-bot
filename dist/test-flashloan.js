"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const web3_js_1 = require("@solana/web3.js");
const FlashLoanRouter_1 = require("./flashloan-advanced/FlashLoanRouter");
async function main() {
    try {
        console.log('Starting Flash Loan Bot Test...');
        // Use devnet for testing
        const connection = new web3_js_1.Connection('https://api.devnet.solana.com', 'confirmed');
        // Generate a test keypair (DO NOT use in production)
        const wallet = web3_js_1.Keypair.generate();
        console.log('Test wallet:', wallet.publicKey.toString());
        // Initialize the router
        const router = new FlashLoanRouter_1.FlashLoanRouter(connection, wallet);
        // Test basic functionality
        const stats = await router.getProviderStats();
        console.log('Provider stats:', stats);
        console.log('Flash loan router initialized successfully!');
    }
    catch (error) {
        console.error('Error:', error);
    }
}
main().catch(console.error);
//# sourceMappingURL=test-flashloan.js.map