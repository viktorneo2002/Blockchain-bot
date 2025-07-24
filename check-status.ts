import { Connection, PublicKey } from '@solana/web3.js';
import * as fs from 'fs';

async function checkStatus() {
  console.log('🔍 Checking bot status...\n');
  
  // Check wallet
  if (fs.existsSync('./wallet.json')) {
    console.log('✅ Wallet file found');
  } else {
    console.log('❌ No wallet file found - run: npx ts-node generate-wallet.ts');
  }
  
  // Check connection
  try {
    const connection = new Connection('https://api.mainnet-beta.solana.com');
    const version = await connection.getVersion();
    console.log('✅ Solana connection working:', version);
  } catch (error) {
    console.log('❌ Cannot connect to Solana');
  }
  
  // Check modules
  const modules = [
    'FlashLoanRouter',
    'VWAPOptimizer', 
    'TWAPExecutor',
    'IcebergOrderManager'
  ];
  
  console.log('\n📦 Module status:');
  for (const module of modules) {
    try {
      require(`./flashloan-advanced/${module}`);
      console.log(`✅ ${module} - OK`);
    } catch (error) {
      console.log(`⚠️  ${module} - Compilation issues`);
    }
  }
}

checkStatus().catch(console.error);
