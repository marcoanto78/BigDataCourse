filename="data1.log"
#**write a command to gzip the file

hadoop fs -copyToLocal "/data/mapreduce/data1/$filename" ./

gzip $filename

today_dt=`date +%Y%m%d`

#check if the directory already exists and capture the return code
hadoop fs -ls /data/archive/$today_dt

if [ $? != 0 ]
then
  #Directory doesn't exists
  #**write command to create directory
  hadoop fs -mkdir /data/archive/$today_dt
else
  #Directory exists. Now lets check if file exists. If so then delete it
  hadoop fs -ls /data/archive/$today_dt/${filename}.gz
  if [$? != 0]
  then
    #**write a command to delete that file
    hadoop fs -rm /data/archive/$today_dt/${filename}.gz
  fi
fi

#Ok, so now we are all set with initial checks. Lets push the file from local to HDFS
#**write a command to move file from Local file system to HDFS
hadoop fs -moveFromLocal ${filename}.gz /data/archive/$today_dt/

