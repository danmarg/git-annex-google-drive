## Google Drive for Git Annex

Yeah, there was already one of these in Python. I wrote a variant. Whatever.

To use, do something like

```
go get github.com/danmarg/git-annex-google-drive
cp git-annex-google-drive /usr/local/bin/git-annex-remote-google-drive
git annex initremote mydrive type=external externaltype=google-drive directory=somepath
```
