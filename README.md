# EPFL_CS451_DistributedAlgorithms

This repo implements Uniform reliable broadcast and causal order broadcast.

Execute
```
g++ -std=c++11 -pthread causal_reliable_broadcast.cpp -o da_proc
```

Then run 
```
./da_proc pid hosts.conf num_msg
```
Feel free to add more process in ```hosts.conf```

The local causality delivery check script is added
