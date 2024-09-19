Scraping categories:  75%|██████████████▎    | 6/8 [00:11<00:04,  2.06s/it, Scraping Engine Upgrade]


2024-09-19 08:19:05.353 [info] > git config --get commit.template [47ms]
2024-09-19 08:19:05.396 [info] > git for-each-ref --format=%(refname)%00%(upstream:short)%00%(objectname)%00%(upstream:track)%00%(upstream:remotename)%00%(upstream:remoteref) --ignore-case refs/heads/main refs/remotes/main [50ms]
2024-09-19 08:19:05.434 [info] > git status -z -uall [34ms]
2024-09-19 08:19:05.455 [info] > git for-each-ref --format=%(refname)%00%(upstream:short)%00%(objectname)%00%(upstream:track)%00%(upstream:remotename)%00%(upstream:remoteref) --ignore-case refs/heads/main refs/remotes/main [12ms]
2024-09-19 08:19:05.523 [info] > git config --local branch.main.vscode-merge-base [66ms]
2024-09-19 08:19:05.523 [warning] [Git][config] git config failed: Failed to execute git
2024-09-19 08:19:05.635 [info] > git reflog main --grep-reflog=branch: Created from *. [110ms]
2024-09-19 08:19:05.647 [info] > git ls-files --stage -- /Users/krisztianimre/parsley/gateio_get_article_list.py [15ms]
2024-09-19 08:19:05.649 [info] > git show --textconv :gateio_get_article_list.py [19ms]
2024-09-19 08:19:05.650 [info] > git symbolic-ref --short refs/remotes/origin/HEAD [13ms]
2024-09-19 08:19:05.650 [info] fatal: ref refs/remotes/origin/HEAD is not a symbolic ref
2024-09-19 08:19:05.670 [info] > git cat-file -s 6097ad6c4912a086fff63545e8fe42fd8ca77d27 [21ms]
2024-09-19 08:19:13.853 [info] > git ls-files --stage -- /Users/krisztianimre/parsley/gateio_article_list.tsv [10ms]
2024-09-19 08:19:13.854 [info] > git show --textconv :gateio_article_list.tsv [12ms]
2024-09-19 08:19:13.864 [info] > git cat-file -s 48f4baa2ce82c07a12ea08f0706bea8dc699ab76 [10ms]
2024-09-19 08:20:08.145 [info] > git show --textconv :gateio_get_article_list.py [14ms]
2024-09-19 08:20:08.145 [info] > git ls-files --stage -- /Users/krisztianimre/parsley/gateio_get_article_list.py [11ms]
2024-09-19 08:20:08.159 [info] > git cat-file -s 6097ad6c4912a086fff63545e8fe42fd8ca77d27 [12ms]
2024-09-19 08:22:32.046 [info] > git config --get commit.template [23ms]
2024-09-19 08:22:32.047 [info] > git for-each-ref --format=%(refname)%00%(upstream:short)%00%(objectname)%00%(upstream:track)%00%(upstream:remotename)%00%(upstream:remoteref) --ignore-case refs/heads/main refs/remotes/main [23ms]
2024-09-19 08:22:32.061 [info] > git status -z -uall [13ms]
2024-09-19 08:22:32.075 [info] > git for-each-ref --format=%(refname)%00%(upstream:short)%00%(objectname)%00%(upstream:track)%00%(upstream:remotename)%00%(upstream:remoteref) --ignore-case refs/heads/main refs/remotes/main [13ms]
2024-09-19 08:22:32.085 [info] > git config --local branch.main.vscode-merge-base [9ms]
2024-09-19 08:22:32.086 [warning] [Git][config] git config failed: Failed to execute git
2024-09-19 08:22:32.106 [info] > git reflog main --grep-reflog=branch: Created from *. [20ms]
2024-09-19 08:22:32.116 [info] > git symbolic-ref --short refs/remotes/origin/HEAD [10ms]
2024-09-19 08:22:32.116 [info] fatal: ref refs/remotes/origin/HEAD is not a symbolic ref



Scraping categories:  75%|██████████████▎    | 6/8 [00:11<00:04,  2.06s/it, Scraping Engine Upgrade]Error fetching https://www.gate.io/announcements/engine-upgrade (attempt 1/5): HTTPSConnectionPool(host='www.gate.io', port=443): Max retries exceeded with url: /announcements/engine-upgrade (Caused by ConnectTimeoutError(<urllib3.connection.HTTPSConnection object at 0x11b821190>, 'Connection to www.gate.io timed out. (connect timeout=None)')). Retrying in 1.00 seconds.
Error fetching https://www.gate.io/announcements/engine-upgrade (attempt 2/5): HTTPSConnectionPool(host='www.gate.io', port=443): Max retries exceeded with url: /announcements/engine-upgrade (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x11b821220>: Failed to resolve 'www.gate.io' ([Errno 8] nodename nor servname provided, or not known)")). Retrying in 2.00 seconds.
Error fetching https://www.gate.io/announcements/engine-upgrade (attempt 3/5): HTTPSConnectionPool(host='www.gate.io', port=443): Max retries exceeded with url: /announcements/engine-upgrade (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x11b823da0>: Failed to resolve 'www.gate.io' ([Errno 8] nodename nor servname provided, or not known)")). Retrying in 4.00 seconds.
Error fetching https://www.gate.io/announcements/engine-upgrade (attempt 4/5): HTTPSConnectionPool(host='www.gate.io', port=443): Max retries exceeded with url: /announcements/engine-upgrade (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x11b823530>: Failed to resolve 'www.gate.io' ([Errno 8] nodename nor servname provided, or not known)")). Retrying in 8.00 seconds.
Error fetching https://www.gate.io/announcements/engine-upgrade (attempt 5/5): HTTPSConnectionPool(host='www.gate.io', port=443): Max retries exceeded with url: /announcements/engine-upgrade (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x11b822c00>: Failed to resolve 'www.gate.io' ([Errno 8] nodename nor servname provided, or not known)")). Retrying in 16.00 seconds.

