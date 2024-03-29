


#Resources Check
- Ondemand master 1 , 3 slave 2xlarge
![img.png](img.png)

#To Run Terraform Script
- In order to run terraform script
- Make relevant changes in prod.tfvars
- ![img_1.png](img_1.png)
- cd emr
- terraform init
- terraform plan --var-file=prod.tfvars
- terraform apply --var-file=prod.tfvars

#PySpark Script
- As mentioned in requirement i generate 1000 records, but you can check it with just a paramter here
- ![img_2.png](img_2.png)
- Instead of 1000 , you can pass any value

#To Run Pyspark Script and Test
- As EMR is always behined the vpc for security reason , you just need to allow your own ip
- In order to allow follow these 2 steps
- ![img_3.png](img_3.png)
- click on security group for master
-select your sg![img_4.png](img_4.png)
-click on inbound rule and add new rule , allow all traffic from my ip
-![img_5.png](img_5.png)
-do same for slave sg ![img_6.png](img_6.png)
- ![img_7.png](img_7.png)
![img_8.png](img_8.png)-
- Now you can access any application ![img_9.png](img_9.png)
-![img_10.png](img_10.png)
-In my case , jupyterHub ip is https://ec2-184-72-173-61.compute-1.amazonaws.com:9443/
- Username is jovyan
- Passowrd is  jupyter
- ![img_11.png](img_11.png)



#Execution Time
- As spark is lazy evualation that's why i added .show()
![img_12.png](img_12.png)
- when i display 1000 rows it takes
![img_13.png](img_13.png)
![img_14.png](img_14.png)

- query 2
![img_15.png](img_15.png)
- so i changed query from AND to OR
![img_16.png](img_16.png)
- ![img_17.png](img_17.png) 

```aidl

while 1 == 1:
    current_sec = timer()
    if current_sec == 30:
        break
    profit_with_date_df.count()
    
```
![img_18.png](img_18.png)