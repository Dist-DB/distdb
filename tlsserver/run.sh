cargo build --release
killall tlsserver
./../target/release/tlsserver datadir=./data &