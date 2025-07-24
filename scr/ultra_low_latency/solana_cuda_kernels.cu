#include <cuda_runtime.h>
#include <stdint.h>
#include <stdio.h>

#define WARP_SIZE 32
#define MAX_ACCOUNTS 64
#define MAX_INSTRUCTIONS 48
#define MAX_DATA_SIZE 1024
#define SIGNATURE_SIZE 64
#define PUBKEY_SIZE 32
#define BLOCKHASH_SIZE 32

struct CudaTransaction {
    uint8_t recent_blockhash[32];
    uint8_t payer_index;
    uint8_t signature_count;
    uint8_t instruction_count;
    uint8_t account_count;
    uint64_t priority_fee;
    uint32_t compute_units;
    uint64_t sequence_id;
    uint32_t message_size;
    uint32_t signature_offset;
    uint32_t accounts_offset;
    uint32_t instructions_offset;
    uint64_t timestamp_ns;
    uint64_t expected_slot;
    uint8_t retry_count;
    uint8_t transaction_type;
    uint8_t padding[14];
};

struct CudaAccount {
    uint8_t pubkey[32];
    uint8_t is_signer;
    uint8_t is_writable;
    uint8_t is_fee_payer;
    uint8_t padding[13];
};

struct CudaInstruction {
    uint8_t program_id_index;
    uint8_t accounts_count;
    uint16_t data_len;
    uint32_t accounts_offset;
    uint32_t data_offset;
    uint32_t compute_units;
    uint8_t padding[4];
};

__device__ __forceinline__ void write_compact_u16(uint8_t* buffer, uint32_t* offset, uint16_t value) {
    if (value < 0x80) {
        buffer[(*offset)++] = value;
    } else if (value < 0x4000) {
        buffer[(*offset)++] = (value & 0x7F) | 0x80;
        buffer[(*offset)++] = value >> 7;
    } else {
        buffer[(*offset)++] = (value & 0x7F) | 0x80;
        buffer[(*offset)++] = ((value >> 7) & 0x7F) | 0x80;
        buffer[(*offset)++] = value >> 14;
    }
}

__device__ __forceinline__ void write_bytes(uint8_t* dest, const uint8_t* src, uint32_t len, uint32_t* offset) {
    for (uint32_t i = threadIdx.x % WARP_SIZE; i < len; i += WARP_SIZE) {
        dest[*offset + i] = src[i];
    }
    if (threadIdx.x % WARP_SIZE == 0) {
        *offset += len;
    }
    __syncwarp();
}

__device__ void build_message_header(
    uint8_t* output,
    uint32_t* offset,
    const CudaTransaction* tx,
    const CudaAccount* accounts
) {
    uint8_t num_required_signatures = 0;
    uint8_t num_readonly_signed = 0;
    uint8_t num_readonly_unsigned = 0;

    for (int i = 0; i < tx->account_count; i++) {
        if (accounts[tx->accounts_offset + i].is_signer) {
            num_required_signatures++;
            if (!accounts[tx->accounts_offset + i].is_writable) {
                num_readonly_signed++;
            }
        } else if (!accounts[tx->accounts_offset + i].is_writable) {
            num_readonly_unsigned++;
        }
    }

    output[(*offset)++] = num_required_signatures;
    output[(*offset)++] = num_readonly_signed;
    output[(*offset)++] = num_readonly_unsigned;
}

__device__ void build_account_keys(
    uint8_t* output,
    uint32_t* offset,
    const CudaTransaction* tx,
    const CudaAccount* accounts
) {
    write_compact_u16(output, offset, tx->account_count);
    
    for (int i = 0; i < tx->account_count; i++) {
        const CudaAccount* account = &accounts[tx->accounts_offset + i];
        for (int j = 0; j < PUBKEY_SIZE; j++) {
            output[(*offset)++] = account->pubkey[j];
        }
    }
}

__device__ void build_blockhash(
    uint8_t* output,
    uint32_t* offset,
    const CudaTransaction* tx
) {
    for (int i = 0; i < BLOCKHASH_SIZE; i++) {
        output[(*offset)++] = tx->recent_blockhash[i];
    }
}

__device__ void build_instructions(
    uint8_t* output,
    uint32_t* offset,
    const CudaTransaction* tx,
    const CudaInstruction* instructions,
    uint8_t* instruction_data
) {
    write_compact_u16(output, offset, tx->instruction_count);
    
    for (int i = 0; i < tx->instruction_count; i++) {
        const CudaInstruction* inst = &instructions[tx->instructions_offset + i];
        
        output[(*offset)++] = inst->program_id_index;
        
        write_compact_u16(output, offset, inst->accounts_count);
        for (int j = 0; j < inst->accounts_count; j++) {
            output[(*offset)++] = j;
        }
        
        write_compact_u16(output, offset, inst->data_len);
        for (int j = 0; j < inst->data_len; j++) {
            output[(*offset)++] = instruction_data[inst->data_offset + j];
        }
    }
}

__device__ void add_compute_budget_instructions(
    uint8_t* output,
    uint32_t* offset,
    uint32_t compute_units,
    uint64_t priority_fee,
    uint8_t compute_budget_program_index
) {
    // SetComputeUnitLimit instruction
    output[(*offset)++] = compute_budget_program_index;
    output[(*offset)++] = 0; // no accounts
    output[(*offset)++] = 9; // data length
    output[(*offset)++] = 2; // SetComputeUnitLimit discriminator
    
    // Write compute units as u32 little-endian
    *((uint32_t*)&output[*offset]) = compute_units;
    *offset += 4;
    
    // 4 bytes padding
    *offset += 4;
    
    // SetComputeUnitPrice instruction
    output[(*offset)++] = compute_budget_program_index;
    output[(*offset)++] = 0; // no accounts
    output[(*offset)++] = 9; // data length
    output[(*offset)++] = 3; // SetComputeUnitPrice discriminator
    
    // Write priority fee as u64 little-endian
    *((uint64_t*)&output[*offset]) = priority_fee;
    *offset += 8;
}

__global__ void build_transactions_kernel(
    CudaTransaction* transactions,
    CudaAccount* accounts,
    CudaInstruction* instructions,
    uint8_t* output_buffer,
    uint32_t batch_size
) {
    uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= batch_size) return;
    
    CudaTransaction* tx = &transactions[tid];
    uint32_t output_offset = tid * 1232;
    uint8_t* tx_output = &output_buffer[output_offset];
    
    uint32_t offset = 2;
    
    // Reserve space for transaction size
    uint16_t* size_ptr = (uint16_t*)tx_output;
    
    // Build message header
    build_message_header(tx_output, &offset, tx, accounts);
    
    // Add account keys
    build_account_keys(tx_output, &offset, tx, accounts);
    
    // Add recent blockhash
    build_blockhash(tx_output, &offset, tx);
    
    // Build instructions with compute budget
    uint8_t total_instructions = tx->instruction_count + 2;
    write_compact_u16(tx_output, &offset, total_instructions);
    
    // Add compute budget instructions first
    uint8_t compute_budget_index = tx->account_count;
    add_compute_budget_instructions(
        tx_output, 
        &offset, 
        tx->compute_units, 
        tx->priority_fee, 
        compute_budget_index
    );
    
    // Add regular instructions
    for (int i = 0; i < tx->instruction_count; i++) {
        CudaInstruction* inst = &instructions[tx->instructions_offset + i];
        
        tx_output[offset++] = inst->program_id_index;
        
        write_compact_u16(tx_output, &offset, inst->accounts_count);
        for (int j = 0; j < inst->accounts_count; j++) {
            tx_output[offset++] = j;
        }
        
        write_compact_u16(tx_output, &offset, inst->data_len);
        offset += inst->data_len;
    }
    
    // Write final size
    *size_ptr = offset - 2;
    
    __threadfence();
}

__global__ void verify_transactions_kernel(
    uint8_t* transactions,
    uint32_t* valid_flags,
    uint32_t batch_size
) {
    uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= batch_size) return;
    
    uint32_t offset = tid * 1232;
    uint16_t size = *((uint16_t*)&transactions[offset]);
    
    valid_flags[tid] = (size > 0 && size < 1230) ? 1 : 0;
}

extern "C" {
    __host__ int cuda_init_context(int device_id) {
        cudaError_t err = cudaSetDevice(device_id);
        if (err != cudaSuccess) {
            return -1;
        }
        
        cudaDeviceProp prop;
        cudaGetDeviceProperties(&prop, device_id);
        
        if (prop.major < 7) {
            return -2;
        }
        
        size_t heap_size = 128 * 1024 * 1024;
        cudaDeviceSetLimit(cudaLimitMallocHeapSize, heap_size);
        
        return 0;
    }
    
    __host__ int cuda_destroy_context() {
        cudaDeviceReset();
        return 0;
    }
    
    __host__ void* cuda_allocate_memory(size_t size) {
        void* ptr;
        cudaError_t err = cudaMalloc(&ptr, size);
        if (err != cudaSuccess) {
            return nullptr;
        }
        cudaMemset(ptr, 0, size);
        return ptr;
    }
    
    __host__ int cuda_free_memory(void* ptr) {
        cudaError_t err = cudaFree(ptr);
        return (err == cudaSuccess) ? 0 : -1;
    }
    
    __host__ int cuda_memcpy_host_to_device(void* device, const void* host, size_t size) {
        cudaError_t err = cudaMemcpy(device, host, size, cudaMemcpyHostToDevice);
        return (err == cudaSuccess) ? 0 : -1;
    }
    
    __host__ int cuda_memcpy_device_to_host(void* host, const void* device, size_t size) {
        cudaError_t err = cudaMemcpy(host, device, size, cudaMemcpyDeviceToHost);
        return (err == cudaSuccess) ? 0 : -1;
    }
    
    __host__ int cuda_launch_transaction_builder(
        CudaTransaction* d_transactions,
        CudaAccount* d_accounts,
        CudaInstruction* d_instructions,
        uint8_t* d_output,
        uint32_t batch_size,
        uint32_t block_size
    ) {
        uint32_t grid_size = (batch_size + block_size - 1) / block_size;
        
        build_transactions_kernel<<<grid_size, block_size>>>(
            d_transactions,
            d_accounts,
            d_instructions,
            d_output,
            batch_size
        );
        
        cudaError_t err = cudaGetLastError();
        return (err == cudaSuccess) ? 0 : -1;
    }
    
    __host__ int cuda_synchronize() {
        cudaError_t err = cudaDeviceSynchronize();
        return (err == cudaSuccess) ? 0 : -1;
    }
    
    __host__ int cuda_get_last_error() {
        cudaError_t err = cudaGetLastError();
        return (int)err;
    }
}
