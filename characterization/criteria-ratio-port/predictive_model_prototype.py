import pandas as pd, glob, math
base='/tmp/moves-defaultdb/movesdb20241112'
def load(n):
    fs=[f for f in glob.glob(base+'/**/*.parquet',recursive=True) if f.lower().split('/')[-1].replace('.parquet','')==n.lower()]
    return pd.read_parquet(fs[0]) if fs else None
ff=load('FuelFormulation').set_index('fuelFormulationID')
def props(form, sulfur_override=None):
    r=ff.loc[form]; g=lambda c: float(r[c]) if c in r and pd.notna(r[c]) else 0.0
    E200=g('e200'); E300=g('e300')
    S=g('sulfurLevel') if sulfur_override is None else sulfur_override
    return dict(Oxygen=0.3653*g('ETOHVolume'),Sulfur=S,RVP=g('RVP'),E200=E200,E300=E300,
        Aromatics=g('aromaticContent'),Olefins=g('olefinContent'),Benzene=g('benzeneContent'),
        T50=2.0408163*(147.91-E200),T90=4.5454545*(155.47-E300),Hi=1.0,Intercept=1.0)
PID={'Oxygen':1,'Sulfur':2,'RVP':3,'E200':4,'E300':5,'Aromatics':6,'Olefins':7,'Benzene':8,'T50':9,'T90':10,'Hi':13,'Intercept':14}
CMP={1:['Oxygen'],2:['Sulfur'],3:['RVP'],4:['E200'],5:['E300'],6:['Aromatics'],7:['Olefins'],8:['Benzene'],
 9:['Oxygen','Oxygen'],10:['Sulfur','Sulfur'],11:['RVP','RVP'],12:['E200','E200'],13:['E300','E300'],14:['Aromatics','Aromatics'],
 15:['Olefins','Olefins'],16:['Benzene','Benzene'],17:['Oxygen','Sulfur'],18:['Oxygen','RVP'],19:['Oxygen','E200'],20:['Oxygen','E300'],
 21:['Oxygen','Aromatics'],22:['Oxygen','Olefins'],23:['Oxygen','Benzene'],24:['Sulfur','RVP'],25:['Sulfur','E200'],26:['Sulfur','E300'],
 27:['Sulfur','Aromatics'],28:['Sulfur','Olefins'],29:['Sulfur','Benzene'],30:['RVP','E200'],31:['RVP','E300'],32:['RVP','Aromatics'],
 33:['RVP','Olefins'],34:['RVP','Benzene'],35:['E200','E300'],36:['E200','Aromatics'],37:['E200','Olefins'],38:['E200','Benzene'],
 39:['E300','Aromatics'],40:['E300','Olefins'],41:['E300','Benzene'],42:['Aromatics','Olefins'],43:['Aromatics','Benzene'],44:['Olefins','Benzene'],
 52:['Intercept'],53:['Hi'],54:['T50'],55:['T90'],56:['T90','T90'],57:['T50','T50'],58:['Oxygen','T90'],59:['Sulfur','Hi'],
 60:['Aromatics','T90'],61:['T50','Hi'],62:['Olefins','T90'],63:['Oxygen','T50'],64:['Sulfur','T90']}
cmp_params=load('complexModelParameters');mfp=load('meanFuelParameters');wt=load('fuelModelWtFactor')
POLPROC=301;FT=1;MYG=19502000;TARGET=9114;BASE=99;models=[302,303,304,305,306,307]
ctr={}
for src in [MYG,0]:
    sub=mfp[(mfp.polProcessID==POLPROC)&(mfp.fuelTypeID==FT)&(mfp.modelYearGroupID==src)]
    for _,row in sub.iterrows(): ctr.setdefault(int(row['fuelParameterID']),float(row['centeringValue']) if pd.notna(row['centeringValue']) else 0.0)
def center(nm): return ctr.get(PID[nm],0.0)
def cmpval(cmpid,p):
    names=CMP.get(cmpid);
    if not names: return 0.0
    v=1.0
    for nm in names: v*=(p[nm]-center(nm))
    return v
def sums(p):
    out={}
    for fm in models:
        s=0.0
        cm=cmp_params[(cmp_params.polProcessID==POLPROC)&(cmp_params.fuelModelID==fm)]
        for _,row in cm.iterrows():
            coeff=float(row['coeff1'])+float(row['coeff2'])+float(row['coeff3'])
            if coeff!=0: s+=coeff*cmpval(int(row['cmpID']),p)
        out[fm]=s
    return out
base_sulfur=props(BASE)['Sulfur']
# ratioNoSulfur: alias forces target sulfur = base sulfur
pt=props(TARGET, sulfur_override=base_sulfur); pb=props(BASE)
ts=sums(pt); bs=sums(pb)
def rns(age):
    num=den=0.0
    for fm in models:
        w=wt[(wt.fuelModelID==fm)&(wt.modelYearGroupID==0)&(wt.ageID==age)]
        wv=float(w['fuelModelWtFactor'].iloc[0]) if len(w) else 0.0
        num+=wv*math.exp(ts[fm]);den+=wv*math.exp(bs[fm])
    return num/den if den else float('nan')
# sulfur model (MY<=2000: IRfactor=0, longCoeff=1 -> sulfAdj3 = sulfShortTarget/sulfShort30 clamped to minSulfAdjust)
sulfurCoeff=0.020830; sulfurBasis=30.0; minSulfAdjust=0.50  # base sulfur 90>30 -> 0.50
def sulfAdj3(sulflevel):
    sst=math.exp(sulfurCoeff*math.log(sulflevel)) if sulflevel>0 else 0
    ss30=math.exp(sulfurCoeff*math.log(sulfurBasis))
    a2=(sst-ss30)/ss30
    v=1.0+a2
    return max(v,minSulfAdjust)
tgt_sulfur=props(TARGET)['Sulfur']
sa_t=sulfAdj3(tgt_sulfur); sa_b=sulfAdj3(base_sulfur)
sulf_eff=max(sa_t/sa_b, minSulfAdjust)
r0=rns(0)
print('base_sulfur=%.2f target_sulfur=%.2f'%(base_sulfur,tgt_sulfur))
print('ratioNoSulfur(alias)=%.6f'%r0)
print('sulfAdj3 target=%.6f base=%.6f  sulf_eff=%.6f'%(sa_t,sa_b,sulf_eff))
print('FINAL ratio = %.6f   (canon 0.962413)'%(sulf_eff*r0))
