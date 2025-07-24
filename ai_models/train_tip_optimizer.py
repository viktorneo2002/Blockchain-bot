{
  "model_config": {
    "xgboost_params": {
      "booster": "gbtree",
      "objective": "reg:squarederror",
      "eval_metric": ["rmse", "mae"],
      "max_depth": 12,
      "min_child_weight": 3,
      "gamma": 0.15,
      "subsample": 0.85,
      "colsample_bytree": 0.8,
      "colsample_bylevel": 0.75,
      "colsample_bynode": 0.8,
      "alpha": 0.1,
      "lambda": 2.0,
      "eta": 0.03,
      "tree_method": "hist",
      "grow_policy": "depthwise",
      "max_leaves": 0,
      "max_bin": 512,
      "predictor": "cpu_predictor",
      "num_parallel_tree": 1,
      "monotone_constraints": "(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)",
      "interaction_constraints": "",
      "multi_strategy": "one_output_per_tree"
    },
    "training_params": {
      "num_boost_round": 500,
      "early_stopping_rounds": 50,
      "verbose_eval": false,
      "callbacks": ["early_stop", "checkpoint"],
      "checkpoint_interval": 25,
      "checkpoint_path": "/var/lib/solana-bot/models/checkpoints/"
    },
    "ensemble_config": {
      "enable_ensemble": true,
      "models": [
        {
          "weight": 0.4,
          "feature_subset": "priority_features"
        },
        {
          "weight": 0.35,
          "feature_subset": "network_features"
        },
        {
          "weight": 0.25,
          "feature_subset": "market_features"
        }
      ]
    }
  },
  "feature_engineering": {
    "priority_features": [
      "slot_height",
      "recent_block_hash_age",
      "compute_unit_price",
      "compute_unit_limit",
      "total_account_locks",
      "write_lock_contention",
      "program_cache_hits",
      "transaction_size_bytes",
      "signature_count",
      "instruction_count"
    ],
    "network_features": [
      "current_slot_leader",
      "leader_schedule_slot_offset",
      "recent_blockhash_queue_len",
      "banking_stage_errors_per_slot",
      "forward_transactions_count",
      "forwarded_packets_per_second",
      "tpu_socket_receive_buffer_size",
      "stake_weighted_qos_priority",
      "connection_cache_stats",
      "quic_connection_attempts"
    ],
    "market_features": [
      "gas_price_percentile_50",
      "gas_price_percentile_90",
      "gas_price_percentile_99",
      "recent_priority_fee_median",
      "slot_success_rate",
      "program_success_rate",
      "mempool_transaction_count",
      "pending_transaction_age_ms",
      "confirmed_transaction_count_10s",
      "failed_transaction_count_10s"
    ],
    "derived_features": {
      "priority_fee_to_success_ratio": {
        "formula": "priority_fee / (slot_success_rate + 0.001)",
        "normalize": true
      },
      "compute_efficiency_score": {
        "formula": "(compute_unit_price * 1000000) / (compute_unit_limit + 1)",
        "normalize": true
      },
      "network_congestion_index": {
        "formula": "(banking_stage_errors_per_slot + forward_transactions_count) / (confirmed_transaction_count_10s + 1)",
        "normalize": true
      },
      "lock_contention_factor": {
        "formula": "write_lock_contention / (total_account_locks + 1)",
        "normalize": true
      },
      "leader_distance_factor": {
        "formula": "leader_schedule_slot_offset / 432000",
        "normalize": false
      }
    },
    "feature_scaling": {
      "method": "robust",
      "quantile_range": [5, 95],
      "handle_outliers": true,
      "outlier_threshold": 4.5
    }
  },
  "optimization_strategy": {
    "tip_calculation": {
      "base_tip_lamports": 5000,
      "min_tip_lamports": 1000,
      "max_tip_lamports": 5000000,
      "dynamic_adjustment": {
        "enable": true,
        "adjustment_factors": {
          "network_congestion": {
            "weight": 0.35,
            "min_multiplier": 0.5,
            "max_multiplier": 3.5
          },
          "priority_percentile": {
            "weight": 0.25,
            "target_percentile": 85,
            "adjustment_rate": 0.15
          },
          "recent_success_rate": {
            "weight": 0.2,
            "lookback_slots": 50,
            "min_success_rate": 0.7
          },
          "compute_budget": {
            "weight": 0.2,
            "compute_per_lamport_target": 200
          }
        }
      }
    },
    "prediction_thresholds": {
      "confidence_threshold": 0.75,
      "min_profit_threshold_lamports": 10000,
      "risk_adjusted_threshold": 0.85,
      "outlier_detection_threshold": 3.5
    },
    "adaptive_learning": {
      "enable": true,
      "online_learning_rate": 0.001,
      "feedback_window_slots": 100,
      "model_update_frequency_slots": 500,
      "performance_metrics": [
        "mean_absolute_error",
        "success_rate",
        "profit_per_transaction",
        "tip_efficiency_ratio"
      ]
    }
  },
  "risk_management": {
    "position_limits": {
      "max_tip_per_transaction_lamports": 5000000,
      "max_daily_tip_spend_lamports": 1000000000,
      "max_consecutive_failures": 10,
      "circuit_breaker_threshold": 0.3
    },
    "volatility_adjustment": {
      "enable": true,
      "volatility_window_slots": 200,
      "volatility_multiplier_range": [0.5, 2.0],
      "volatility_calculation_method": "exponential_weighted"
    },
    "drawdown_protection": {
      "max_drawdown_percentage": 15,
      "recovery_period_slots": 1000,
      "reduction_factor": 0.5
    }
  },
  "network_optimization": {
    "rpc_configuration": {
      "commitment_level": "processed",
      "preflight_commitment": "processed",
      "encoding": "base64",
      "max_retries": 3,
      "retry_delay_ms": 50,
      "timeout_ms": 500
    },
    "connection_pooling": {
      "min_idle_connections": 5,
      "max_connections": 20,
      "connection_timeout_ms": 1000,
      "idle_timeout_ms": 30000,
      "max_lifetime_ms": 300000
    },
    "leader_tracking": {
      "enable": true,
      "leader_schedule_cache_duration_slots": 432000,
      "preferred_leaders": [],
      "leader_performance_tracking": true
    }
  },
  "performance_tuning": {
    "cache_configuration": {
      "feature_cache_size_mb": 128,
      "model_cache_entries": 5,
      "prediction_cache_ttl_ms": 100,
      "transaction_history_cache_size": 10000
    },
    "batch_processing": {
      "enable": true,
      "batch_size": 32,
      "max_batch_delay_ms": 10,
      "parallel_predictions": true,
      "thread_pool_size": 8
    },
    "memory_optimization": {
      "gc_interval_predictions": 10000,
      "feature_buffer_reuse": true,
      "prediction_buffer_pool_size": 100
    }
  },
  "monitoring": {
    "metrics_collection": {
      "enable": true,
      "collection_interval_ms": 1000,
      "metrics_retention_hours": 24,
      "export_format": "prometheus"
    },
    "alerting": {
      "enable": true,
      "alert_channels": ["webhook", "log"],
      "alert_thresholds": {
        "model_accuracy_drop": 0.1,
        "success_rate_drop": 0.15,
        "latency_p99_ms": 50,
        "error_rate_threshold": 0.05
      }
    },
    "logging": {
      "level": "info",
      "structured_logging": true,
      "log_predictions": false,
      "log_rotation_size_mb": 100,
      "log_retention_days": 7
    }
  },
  "deployment": {
    "model_versioning": {
      "enable": true,
      "version_strategy": "semantic",
      "rollback_on_performance_drop": true,
      "canary_deployment_percentage": 10
    },
    "hot_reload": {
      "enable": true,
      "model_path": "/var/lib/solana-bot/models/production/",
      "config_reload_interval_seconds": 300
    },
    "failover": {
      "enable": true,
      "fallback_tip_strategy": "percentile_based",
      "fallback_percentile": 75
    }
  }
}
