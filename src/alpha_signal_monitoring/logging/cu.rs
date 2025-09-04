#[macro_export]
macro_rules! profile_cu {
    ($msg:expr, $expr:expr) => {{
        msg!(concat!($msg, " {"));
        sol_log_compute_units();
        let res = { $expr };
        sol_log_compute_units();
        msg!(concat!("} // ", $msg));
        res
    }};
}
