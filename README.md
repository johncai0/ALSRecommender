# ALSRecommender
基于Spark MlLib的交替最小二乘法模型
考虑到生产环境的数据因素，再加一层封装，便于使用。

三步完成推荐
val recommend=new ALSrecommend(ss,df) //初始化ALSrecommend对象，标准化训练数据

recommend.evaluate() //超参数测试和训练模型，并挑出最优的ASL模型

recommend.makeRecommendations("3cd6d42a-4cba-1a22-181c-e4d3ca558e19",5).show(false) //推荐
