#!/bin/bash

for i in {1..8}
do
  gnome-terminal -- bash -c "cd $(pwd); go run mrworker.go wc.so; exec bash"
done
