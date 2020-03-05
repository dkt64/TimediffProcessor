docker cp C:\Users\admin\java\src\timediff-processor\nifi-timediff-nar\target\%1 nifi:/opt/nifi/nifi-current/extensions
docker exec nifi bash /opt/nifi/nifi-current/bin/nifi.sh restart