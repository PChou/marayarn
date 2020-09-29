#初步数据库表格
![](../doc/web-db-v1.jpg)


#应用状态转换
![](../doc/app_status.jpg)


# 文件管理
## 上传(目标位置不能存在同名文件)
### PUT     /api/artifact/upload
* type - `local`，`URI`，`HDFS`,`http`
* file -  type为`local`时传递
* source - type为`URI`或`HDFS`时 传递
* directory - 存放目录 

## 删除
### DELETE      /api/artifact/{id}

## 查询
### GET    /api/artifact/list
* page 
* size
* filter
* sort



#应用
## 创建
### POST /api/application
* name - 名称
* group_id - 分组目录Id
* cpu - cpu 个数
* memory - 内存
* instanceCount - 实例个数 0 代表挂起
* command - 命令
* artifactIds - 引用的文件id列表
* env - 环境变量 map 对象
* constraints - 约束
* user - 认证用户
* queue - 任务队列


## 状态查询
### GET /api/application/query
* status




## 删除
### DELETE /api/application/{id}

## 重启
### POST /api/application/{id}/restart

## scale/suspend （scale 为0即为suspend操作）
### POST /api/application/{id}/scale
* instanceCount - 0 代表 suspend
* killContainerIds - 指定需要被删除的容器


## 获取应用实例信息
### GET /api/application/{id}/instance

## 获取应用配置信息 (即获取历史)
### GET /api/application/{id}/configs





#分组目录管理
## 创建
### POST /api/group
* name - 名称，同级目录下不能重名
* parentId - 父级目录id,默认为 根目录(`-1`)

## 查询（展示子目录和application 摘要信息）
### GET   /api/group/list/{id}

## 删除 
### DELETE /api/group/{id}




