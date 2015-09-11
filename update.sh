today=`date +%Y%m%d`
git add --all
git commit -a -m "$today $1"
git push

