cd /root
umount -f nfs 
mount -o proto=tcp,noacl,wsize=524288,rsize=524288,timeo=30,retrans=1 localhost:/ nfs

