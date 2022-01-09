# D-crontab

##Introduction 
D-contab is a distributed crontab management software that allows you to run crontab commands on different clusters with a few clicks on a website. It supports CRUD operation for jobs and provides a backend website for easy opeartion. The running output are collected by Go and stored in MongoDB. The ETCD is used as underlying communcation tool between master ans workers.

##To run
go run master.go

##To do:
*Support shell file 
*Handle the case when worker goes offline