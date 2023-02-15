#!/bin/sh

./build.sh

if [ $? -ne 0 ]; then
  echo ">> Error building contract"
  exit 1
fi

echo ">> Deploying contract"

near dev-deploy --wasmFile ./target/wasm32-unknown-unknown/release/nearcrowd.wasm 
# --accountId=app.nearcrowd.near --networkId=mainnet --nodeUrl=https://rpc.mainnet.near.org