from sklearn.externals import joblib
import numpy as np

scalers={'classification':{},'regression':{}}
models={'classification':{},'regression':{}}
for ml in ['classification','regression']:
  for k in range(1,5):
    scalers[ml][k]=joblib.load('models/k%d/%s/scaler.pkl'%(k,ml))
    models[ml][k]=joblib.load('models/k%d/%s/model.pkl'%(k,ml))

scaler=scalers['regression'][1]
model=models['regression'][1]
res=np.exp(model.predict(scaler.transform([[[2,6,13]]]))[0])
print(res)
