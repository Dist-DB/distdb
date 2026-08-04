cargo build --release
killall server
./target/release/server datadir=./data -- wss=on tls_san=localhost tls_server=0.0.0.0:5443 &