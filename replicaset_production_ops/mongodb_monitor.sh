#!/bin/bash

ps -A | grep mongod
if [ $? -ne 0 ]
then
   echo "start mongodb....."
   /home/cdeng/mongodb-linux-x86_64-ubuntu1604-3.6.3/bin/mongod 1> /dev/null
   if [ $? -eq 0 ];then
      echo "successfully in sarting mongodb"
   else
      echo "failed to start mongodb"
   fi
else
   echo "runing....."
fi
