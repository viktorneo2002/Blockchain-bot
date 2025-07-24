import { Connection, Keypair, PublicKey, Transaction, SystemProgram } from '@solana/web3.js';
import * as fs from 'fs';

async function testBot() {
  console.log('🧪 Testing basic bot functionality...\n');
  
  try {
    // Load wallet
    const walletData = JSON.parse(fs.readFileSync('./wallet.json', 'utf-8'));
    const wallet = Keypair.fromSecretKey(new Uint8Array(walletData));
    console.log('✅ Wallet loaded:', wallet.publicKey.toString());
    
    // Connect to Solana
    const connection = new Connection('https://api.mainnet-beta.solana.com', 'confirmed');
    const version = await connection.getVersion();
    console.log('✅ Connected to Solana:', version);
    
    // Check balance
    const balance = await connection.getBalance(wallet.publicKey);
    console.log('💰 Balance:', balance / 1e9, 'SOL');
    
    // Get recent blockhash
    const { blockhash } = await connection.getLatestBlockhash();
    console.log('📦 Recent blockhash:', blockhash);
    
    // Check some token prices (USDC)
    const USDC_MINT = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
    const supply = await connection.getTokenSupply(USDC_MINT);
    console.log('💵 USDC Supply:', supply.value.uiAmount?.toLocaleString());
    
    console.log('\n✅ All basic tests passed!');
    console.log('🚀 Bot is ready to run (once funded with SOL)');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

testBot();
