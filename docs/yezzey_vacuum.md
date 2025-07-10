For yezzey vacuum to work, you must esure that extension veriosn is at least 1.8.5.

If you want to simply remove all data related to rows deleted in database, you need yezzey_vacuum_garbage SQL function


```
SELECT yezzey_vacuum_garbage(true, true);
```

This will issue delete-garbage request to yproxy. yproxy will remove all files which are
in extnernal storage and not in yezzey virtual index.

For yproxy to work in this case, you need CheckBackup setting set to off

https://github.com/open-gpdb/yproxy/blob/master/config/vacuum.go#L4

cat >> yproxy.yaml << EOH
vacuum:
  check_backup: false
EOH