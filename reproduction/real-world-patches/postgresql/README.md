# PostgreSQL Real-World Patches

> **_NOTE:_** The experiments can be performed with the regular or the MMView Linux kernel.

> **_DISCLAIMER:_** This toolchain relies on the current state of the PostgreSQL commitfest website and the PostgreSQL git history, so reproducing the results may lead to variations. Since this toolchain uses web scraping, it is also possible that this toolchain may fail (e.g., if the HTML structure of the website is changed). For reference, the data we collected is available in the [original-data](original-data) folder.

We extracted data from the PostgreSQL [commitfest](https://commitfest.postgresql.org) (a periodic event for reviewing, testing, and committing new patches) by identifying URLs linking to the discussions of the commits.
Using these URLs, we crawled the git commit history for commit messages referencing the discussion URLs.

Although the commitfest website often includes code changes as attachments, it does not specify the exact PostgreSQL version these changes should be applied to.
Thus, we relied on the discussion URLs, as they are referenced both on the commitfest website and in the commit messages.

## Details
We only consider commitfests that are marked as "Closed".
Within each commitfest, we focus specifically on the "Bug Fixes" section.
For each patch in this section, we retrieve the link from the "Emails" section to extract the message ID from the discussion on the public mailing list.

Next, we gather all commit messages from the PostgreSQL git repository, specifically looking for entries containing `Discussion: <LINK TO Mailing List>`.
This link is found within the git commit messages and we extract the message ID from it.
We then check if the message ID is present in our list of crawled message IDs.

Finally, we proceed to build each commit and generate the corresponding patch.

```
# 0. Prepare
./setup
# 1. Crawl PostgreSQL commitfest website
# Output: postgresql-message-ids.txt
./crawl-commitfest.py
# 2. Search for all message IDs in the git repository and combine both
# Output: bug-fix-commits.txt
./combine-commitfest-with-git.py
# 3. Build each commit and check whether we can create a patch:
# Output: output/
cd crawl-postgres
./crawl ../bug-fix-commits.txt
# 4. Analyze commits
# Output: real-world.commits.success
./analyze-real-world-success-patch-list output/patches
```

As the script crawls the current commitfests available on the website - which may differ from the state when we initially collected the data — we have provided our original output files in the [original-data](original-data) directory.
The `info-*` files contain the complete output of the respective scripts for reference.

For the share of found and live patchable commits, please see the `real-world-patches-postgresql.txt` file in the  [../../data](../../data) directory (and the README of the `data` directory on how to read the file). 
