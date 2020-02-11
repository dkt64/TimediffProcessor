docker cp C:\Users\admin\java\src\timediff-processor\nifi-timediff-nar\target\nifi-timediff-nar-1.1.nar sandbox-hdf:/usr/hdf/current/nifi/lib
docker exec sandbox-hdf bash /usr/hdf/current/nifi/bin/nifi.sh restart
