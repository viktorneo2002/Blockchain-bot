import { Keypair } from '@solana/web3.js';
import * as fs from 'fs';

const wallet = Keypair.generate();
const walletData = Array.from(wallet.secretKey);

fs.writeFileSync('./wallet.json', JSON.stringify(walletData));
console.log('Wallet generated!');
console.log('Public Key:', wallet.publicKey.toString());
console.log('Saved to: ./wallet.json');
console.log('\n⚠️  IMPORTANT: Fund this wallet with at least 0.05 SOL before running the bot!');
