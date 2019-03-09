import matplotlib.pyplot as plt
import time
import datetime

def groupmeanplot(data,groupvar,xvar,yvars,savepath = "/home/wangx17/data/"):
	import matplotlib.pyplot as plt
	fig, ax = plt.subplots(3,1,figsize=(15,21))
	for i in range(len(yvars)):
		data.groupby([xvar,groupvar])[yvars[i]].mean().unstack().plot(legend=True,ax=ax[i])
		ax[i].set_title(yvars[i])
	plt.savefig(savepath+" "+"&".join(yvars)+"by " + groupvar +"vs" + xvar +".png")
	plt.show()
