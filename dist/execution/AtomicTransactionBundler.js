"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AtomicTransactionBundler = void 0;
const web3_js_1 = require("@solana/web3.js");
const bs58_1 = __importDefault(require("bs58"));
class AtomicTransactionBundler {
    constructor(config = {}) {
        this.config = {
            maxRetries: config.maxRetries || 3,
            simulationRetries: config.simulationRetries || 2,
            priorityFeeMultiplier: config.priorityFeeMultiplier || 1.5,
            computeUnitLimit: config.computeUnitLimit || 1400000,
            jitoTipAmount: config.jitoTipAmount || 10000,
            jitoBlockEngineUrl: config.jitoBlockEngineUrl || 'https://mainnet.block-engine.jito.wtf',
            rpcEndpoint: config.rpcEndpoint || 'https://api.mainnet-beta.solana.com',
            commitment: config.commitment || 'processed',
            skipPreflight: config.skipPreflight !== undefined ? config.skipPreflight : true,
            preflightCommitment: config.preflightCommitment || 'processed',
            maxSignatureWaitTime: config.maxSignatureWaitTime || 30000
        };
        this.connection = new web3_js_1.Connection(this.config.rpcEndpoint, {
            commitment: this.config.commitment,
            confirmTransactionInitialTimeout: 60000
        });
        this.jitoConnection = new web3_js_1.Connection(this.config.jitoBlockEngineUrl, {
            commitment: this.config.commitment,
            confirmTransactionInitialTimeout: 60000
        });
        this.recentBlockhash = '';
        this.lastBlockhashFetch = 0;
        this.blockHashCache = new Map();
    }
    async executeAtomicBundle(bundles) {
        try {
            const transactions = await this.buildVersionedTransactions(bundles);
            const simulationResults = await this.simulateTransactions(transactions);
            if (!this.validateSimulations(simulationResults)) {
                return {
                    success: false,
                    error: new Error('Transaction simulation failed'),
                    simulationLogs: this.extractSimulationLogs(simulationResults)
                };
            }
            const jitoBundle = await this.createJitoBundle(transactions);
            const bundleId = await this.sendJitoBundle(jitoBundle);
            const result = await this.confirmBundle(bundleId, transactions);
            return result;
        }
        catch (error) {
            return {
                success: false,
                error: error instanceof Error ? error : new Error(String(error))
            };
        }
    }
    async buildVersionedTransactions(bundles) {
        const transactions = [];
        const { blockhash, lastValidBlockHeight } = await this.getRecentBlockhash();
        for (const bundle of bundles) {
            const priorityFeeIx = this.createPriorityFeeInstruction(bundle.priorityFee);
            const computeUnitIx = this.createComputeUnitInstruction(bundle.computeUnitLimit);
            const allInstructions = [priorityFeeIx, computeUnitIx, ...bundle.instructions];
            const message = new web3_js_1.MessageV0({
                header: {
                    numRequiredSignatures: 1,
                    numReadonlySignedAccounts: 0,
                    numReadonlyUnsignedAccounts: 0
                },
                staticAccountKeys: [bundle.payer],
                recentBlockhash: blockhash,
                compiledInstructions: allInstructions.map((ix) => ({
                    programIdIndex: 0,
                    accountKeyIndexes: [],
                    data: ix.data
                })),
                addressTableLookups: [],
            });
            const transaction = new web3_js_1.VersionedTransaction(message);
            transaction.sign(bundle.signers);
            transactions.push(transaction);
        }
        return transactions;
    }
    createPriorityFeeInstruction(priorityFee) {
        const fee = priorityFee || this.calculateDynamicPriorityFee();
        return web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: Math.floor(fee * this.config.priorityFeeMultiplier)
        });
    }
    createComputeUnitInstruction(computeUnits) {
        return web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({
            units: computeUnits || this.config.computeUnitLimit
        });
    }
    calculateDynamicPriorityFee() {
        const baseFee = 1000;
        const timestamp = Date.now();
        const hourOfDay = new Date(timestamp).getUTCHours();
        const peakHours = [14, 15, 16, 17, 18, 19, 20, 21];
        const isPeakHour = peakHours.includes(hourOfDay);
        return isPeakHour ? baseFee * 2.5 : baseFee;
    }
    async simulateTransactions(transactions) {
        const results = [];
        for (let i = 0; i < this.config.simulationRetries; i++) {
            try {
                const simulations = await Promise.all(transactions.map(tx => this.connection.simulateTransaction(tx, {
                    sigVerify: true,
                    replaceRecentBlockhash: true,
                    commitment: 'processed'
                })));
                const allValid = simulations.every(sim => !sim.value.err);
                if (allValid) {
                    return simulations.map(s => s.value);
                }
                results.push(...simulations.map(s => s.value));
            }
            catch (error) {
                if (i === this.config.simulationRetries - 1)
                    throw error;
                await this.sleep(100 * (i + 1));
            }
        }
        return results;
    }
    validateSimulations(simulations) {
        return simulations.every(sim => {
            if (sim.err)
                return false;
            if (sim.unitsConsumed && sim.unitsConsumed > this.config.computeUnitLimit * 0.95) {
                return false;
            }
            return true;
        });
    }
    extractSimulationLogs(simulations) {
        return simulations.flatMap(sim => sim.logs || []);
    }
    async createJitoBundle(transactions) {
        const tipAccount = await this.getJitoTipAccount();
        const tipInstruction = this.createJitoTipInstruction(tipAccount);
        const lastTx = transactions[transactions.length - 1];
        const lastMessage = lastTx.message;
        const newInstructions = [...lastMessage.compiledInstructions];
        const blockhash = lastMessage.recentBlockhash;
        const tipIxCompiled = this.compileInstruction(tipInstruction, lastMessage);
        newInstructions.push(tipIxCompiled);
        const updatedMessage = new web3_js_1.MessageV0({
            header: {
                numRequiredSignatures: lastMessage.header.numRequiredSignatures,
                numReadonlySignedAccounts: 0,
                numReadonlyUnsignedAccounts: 0
            },
            staticAccountKeys: [lastMessage.staticAccountKeys[0]],
            recentBlockhash: blockhash,
            compiledInstructions: newInstructions.map(ix => ({
                programIdIndex: 0,
                accountKeyIndexes: [],
                data: ix.data
            })),
            addressTableLookups: [],
        });
        const updatedLastTx = new web3_js_1.VersionedTransaction(updatedMessage);
        updatedLastTx.signatures = lastTx.signatures;
        const bundleTransactions = [...transactions.slice(0, -1), updatedLastTx];
        return bundleTransactions;
    }
    compileInstruction(instruction, message) {
        const programIdIndex = this.findAccountIndex(instruction.programId, message);
        const accounts = instruction.keys.map(key => ({
            pubkey: this.findAccountIndex(key.pubkey, message),
            isSigner: key.isSigner,
            isWritable: key.isWritable
        }));
        return {
            programIdIndex,
            accounts: accounts.map(a => a.pubkey),
            data: bs58_1.default.encode(instruction.data)
        };
    }
    findAccountIndex(pubkey, message) {
        const index = message.staticAccountKeys.findIndex(key => key.equals(pubkey));
        if (index === -1) {
            throw new Error(`Account ${pubkey.toBase58()} not found in message`);
        }
        return index;
    }
    async getJitoTipAccount() {
        const tipAccounts = [
            '96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5',
            'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe',
            'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY',
            'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49',
            'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh',
            'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt',
            'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL',
            '3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT'
        ];
        const randomIndex = Math.floor(Math.random() * tipAccounts.length);
        return new web3_js_1.PublicKey(tipAccounts[randomIndex]);
    }
    createJitoTipInstruction(tipAccount) {
        return {
            keys: [
                { pubkey: tipAccount, isSigner: false, isWritable: true }
            ],
            data: Buffer.from([]),
            programId: new web3_js_1.PublicKey('11111111111111111111111111111111')
        };
    }
    async sendJitoBundle(transactions) {
        for (let attempt = 0; attempt < this.config.maxRetries; attempt++) {
            try {
                const bundleId = await this.submitBundleToJito(transactions);
                return bundleId;
            }
            catch (error) {
                if (attempt === this.config.maxRetries - 1)
                    throw error;
                await this.sleep(Math.pow(2, attempt) * 100);
            }
        }
        throw new Error('Failed to send Jito bundle after max retries');
    }
    async submitBundleToJito(transactions) {
        const serializedBundle = transactions.map((tx) => bs58_1.default.encode(tx.serialize()));
        const response = await fetch(`${this.config.jitoBlockEngineUrl}/api/v1/bundles`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 1,
                method: 'sendBundle',
                params: [serializedBundle]
            })
        });
        const result = await response.json();
        if (result.error) {
            throw new Error(`Jito bundle error: ${result.error.message}`);
        }
        return result.result;
    }
    async confirmBundle(bundleId, transactions) {
        const startTime = Date.now();
        const signatures = transactions.map(tx => bs58_1.default.encode(tx.signatures[0]));
        while (Date.now() - startTime < this.config.maxSignatureWaitTime) {
            try {
                const statuses = await this.connection.getSignatureStatuses(signatures);
                const allConfirmed = statuses.value.every(status => status !== null && status.confirmationStatus === 'confirmed');
                if (allConfirmed) {
                    return {
                        success: true,
                        signatures,
                        slot: statuses.context.slot
                    };
                }
                const anyFailed = statuses.value.some(status => status !== null && status.err !== null);
                if (anyFailed) {
                    return {
                        success: false,
                        error: new Error('Bundle transaction failed'),
                        signatures
                    };
                }
            }
            catch (error) {
                if (Date.now() - startTime >= this.config.maxSignatureWaitTime - 1000) {
                    throw error;
                }
            }
            await this.sleep(500);
        }
        return {
            success: false,
            error: new Error('Bundle confirmation timeout'),
            signatures
        };
    }
    async getRecentBlockhash() {
        const now = Date.now();
        if (this.recentBlockhash && now - this.lastBlockhashFetch < 10000) {
            const cached = this.blockHashCache.get(this.recentBlockhash);
            if (cached)
                return cached;
        }
        try {
            const { blockhash, lastValidBlockHeight } = await this.connection.getLatestBlockhash('finalized');
            this.recentBlockhash = blockhash;
            this.lastBlockhashFetch = now;
            const blockHashData = { blockhash, lastValidBlockHeight };
            this.blockHashCache.set(blockhash, blockHashData);
            if (this.blockHashCache.size > 10) {
                const firstKey = this.blockHashCache.keys().next().value;
                this.blockHashCache.delete(firstKey);
            }
            return blockHashData;
        }
        catch (error) {
            throw new Error(`Failed to get recent blockhash: ${error}`);
        }
    }
    async buildOptimizedTransaction(instructions, payerKey, signers, lookupTables) {
        const { blockhash } = await this.getRecentBlockhash();
        const priorityFee = this.calculateDynamicPriorityFee();
        const computeUnits = await this.estimateComputeUnits(instructions, payerKey);
        const optimizedInstructions = [
            this.createPriorityFeeInstruction(priorityFee),
            this.createComputeUnitInstruction(computeUnits),
            ...instructions
        ];
        const messageV0 = new web3_js_1.MessageV0({
            header: {
                numRequiredSignatures: signers.length,
                numReadonlySignedAccounts: 0,
                numReadonlyUnsignedAccounts: 0
            },
            staticAccountKeys: [payerKey],
            recentBlockhash: blockhash,
            compiledInstructions: optimizedInstructions.map(ix => ({
                programIdIndex: 0,
                accountKeyIndexes: [],
                data: ix.data
            })),
            addressTableLookups: [],
        });
        const transaction = new web3_js_1.VersionedTransaction(messageV0);
        transaction.sign(signers);
        return transaction;
    }
    async estimateComputeUnits(instructions, payerKey) {
        try {
            const testInstructions = [
                web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({ units: 1400000 }),
                ...instructions
            ];
            const { blockhash } = await this.getRecentBlockhash();
            const testMessage = new web3_js_1.MessageV0({
                header: {
                    numRequiredSignatures: 1,
                    numReadonlySignedAccounts: 0,
                    numReadonlyUnsignedAccounts: 0
                },
                staticAccountKeys: [payerKey],
                recentBlockhash: blockhash,
                compiledInstructions: testInstructions.map(ix => ({
                    programIdIndex: 0,
                    accountKeyIndexes: [],
                    data: ix.data
                })),
                addressTableLookups: [],
            });
            const testTx = new web3_js_1.VersionedTransaction(testMessage);
            const simulation = await this.connection.simulateTransaction(testTx, {
                sigVerify: false,
                replaceRecentBlockhash: true
            });
            if (simulation.value.err) {
                return this.config.computeUnitLimit;
            }
            const unitsUsed = simulation.value.unitsConsumed || 200000;
            return Math.min(Math.ceil(unitsUsed * 1.2), this.config.computeUnitLimit);
        }
        catch {
            return this.config.computeUnitLimit;
        }
    }
    async executeWithRetry(operation, retries = this.config.maxRetries) {
        let lastError;
        for (let attempt = 0; attempt < retries; attempt++) {
            try {
                return await operation();
            }
            catch (error) {
                lastError = error instanceof Error ? error : new Error(String(error));
                if (this.isRetryableError(lastError) && attempt < retries - 1) {
                    await this.sleep(Math.pow(2, attempt) * 100);
                    continue;
                }
                throw lastError;
            }
        }
        throw lastError || new Error('Operation failed after retries');
    }
    isRetryableError(error) {
        const retryableMessages = [
            'blockhash not found',
            'socket hang up',
            'ECONNRESET',
            'ETIMEDOUT',
            'rate limit',
            'Service Unavailable',
            'Gateway Timeout'
        ];
        return retryableMessages.some(msg => error.message.toLowerCase().includes(msg.toLowerCase()));
    }
    async bundleAndExecute(transactionSets) {
        const bundles = transactionSets.map(set => ({
            instructions: set.instructions,
            payer: set.signers[0].publicKey,
            signers: set.signers,
            computeUnitLimit: set.computeLimit,
            priorityFee: set.priorityFee
        }));
        return this.executeWithRetry(() => this.executeAtomicBundle(bundles));
    }
    async validateBundleAccounts(bundles) {
        const allAccounts = new Set();
        for (const bundle of bundles) {
            for (const instruction of bundle.instructions) {
                for (const key of instruction.keys) {
                    allAccounts.add(key.pubkey.toBase58());
                }
            }
        }
        try {
            const accountInfos = await this.connection.getMultipleAccountsInfo(Array.from(allAccounts).map(a => new web3_js_1.PublicKey(a)));
            return accountInfos.every(info => info !== null);
        }
        catch {
            return false;
        }
    }
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    updateConfig(newConfig) {
        this.config = { ...this.config, ...newConfig };
        if (newConfig.rpcEndpoint) {
            this.connection = new web3_js_1.Connection(newConfig.rpcEndpoint, {
                commitment: this.config.commitment,
                confirmTransactionInitialTimeout: 60000
            });
        }
        if (newConfig.jitoBlockEngineUrl) {
            this.jitoConnection = new web3_js_1.Connection(newConfig.jitoBlockEngineUrl, {
                commitment: this.config.commitment,
                confirmTransactionInitialTimeout: 60000
            });
        }
    }
    getConfig() {
        return { ...this.config };
    }
    async healthCheck() {
        try {
            const [mainnetHealth, jitoHealth] = await Promise.all([
                this.connection.getVersion(),
                this.jitoConnection.getVersion()
            ]);
            return !!(mainnetHealth && jitoHealth);
        }
        catch {
            return false;
        }
    }
    async getTransactionCost(instructions, priorityFee) {
        const fee = priorityFee || this.calculateDynamicPriorityFee();
        const computeUnits = await this.estimateComputeUnits(instructions, instructions[0].keys[0].pubkey);
        const priorityFeeLamports = Math.ceil((fee * computeUnits) / 1000000);
        const baseFee = 5000; // Base transaction fee
        const jitoTip = this.config.jitoTipAmount;
        return baseFee + priorityFeeLamports + jitoTip;
    }
    clearBlockhashCache() {
        this.blockHashCache.clear();
        this.recentBlockhash = '';
        this.lastBlockhashFetch = 0;
    }
    async monitorBundleStatus(signatures) {
        const statusMap = new Map();
        try {
            const statuses = await this.connection.getSignatureStatuses(signatures, {
                searchTransactionHistory: true
            });
            signatures.forEach((sig, index) => {
                statusMap.set(sig, statuses.value[index]);
            });
            return statusMap;
        }
        catch (error) {
            signatures.forEach(sig => statusMap.set(sig, null));
            return statusMap;
        }
    }
    async getOptimalSlot() {
        const slot = await this.connection.getSlot('finalized');
        return slot + 2; // Target 2 slots ahead for optimal landing
    }
    validateTransactionSize(transaction) {
        const serialized = transaction.serialize();
        return serialized.length <= 1232; // Max transaction size
    }
    async preBundleValidation(bundles) {
        const errors = [];
        try {
            // Validate account existence
            const accountsValid = await this.validateBundleAccounts(bundles);
            if (!accountsValid) {
                errors.push('One or more accounts do not exist');
            }
            // Build and validate transaction sizes
            const transactions = await this.buildVersionedTransactions(bundles);
            for (let i = 0; i < transactions.length; i++) {
                if (!this.validateTransactionSize(transactions[i])) {
                    errors.push(`Transaction ${i} exceeds size limit`);
                }
            }
            // Validate signers
            for (let i = 0; i < bundles.length; i++) {
                if (bundles[i].signers.length === 0) {
                    errors.push(`Bundle ${i} has no signers`);
                }
            }
            return {
                valid: errors.length === 0,
                errors
            };
        }
        catch (error) {
            errors.push(`Validation error: ${error}`);
            return { valid: false, errors };
        }
    }
    getConnectionStats() {
        return {
            mainnet: this.config.rpcEndpoint,
            jito: this.config.jitoBlockEngineUrl,
            cacheSize: this.blockHashCache.size
        };
    }
}
exports.AtomicTransactionBundler = AtomicTransactionBundler;
exports.default = AtomicTransactionBundler;
//# sourceMappingURL=AtomicTransactionBundler.js.map