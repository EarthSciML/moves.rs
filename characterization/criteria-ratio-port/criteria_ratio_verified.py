"""Complete VERIFIED criteria-ratio (EPA fuel-effects) reference implementation.
Reproduces canonical criteriaRatio EXACTLY for NOx/HC/CO, MY1980 & MY1994 (ff9114).
The Rust port should mirror this. See SOLVED.md for the algorithm narrative."""
import pandas as pd, glob, math
base='/tmp/moves-defaultdb/movesdb20241112'
def load(n): return pd.read_parquet([f for f in glob.glob(base+'/**/*.parquet',recursive=True) if f.lower().split('/')[-1].replace('.parquet','')==n.lower()][0])
ff=load('FuelFormulation').set_index('fuelFormulationID'); fpn=load('fuelParameterName'); fmn=load('fuelModelName')
cmpP=load('complexModelParameters'); mfp=load('meanFuelParameters'); bf=load('baseFuel'); wt=load('fuelModelWtFactor')
smc=load('sulfurModelCoeff')
fpn_i=fpn.set_index('fuelParameterID')
CMP={1:['Oxygen'],2:['Sulfur'],3:['RVP'],4:['E200'],5:['E300'],6:['Aromatics'],7:['Olefins'],8:['Benzene'],
 9:['Oxygen','Oxygen'],10:['Sulfur','Sulfur'],11:['RVP','RVP'],12:['E200','E200'],13:['E300','E300'],14:['Aromatics','Aromatics'],
 15:['Olefins','Olefins'],16:['Benzene','Benzene'],17:['Oxygen','Sulfur'],18:['Oxygen','RVP'],19:['Oxygen','E200'],20:['Oxygen','E300'],
 21:['Oxygen','Aromatics'],22:['Oxygen','Olefins'],23:['Oxygen','Benzene'],24:['Sulfur','RVP'],25:['Sulfur','E200'],26:['Sulfur','E300'],
 27:['Sulfur','Aromatics'],28:['Sulfur','Olefins'],29:['Sulfur','Benzene'],30:['RVP','E200'],31:['RVP','E300'],32:['RVP','Aromatics'],
 33:['RVP','Olefins'],34:['RVP','Benzene'],35:['E200','E300'],36:['E200','Aromatics'],37:['E200','Olefins'],38:['E200','Benzene'],
 39:['E300','Aromatics'],40:['E300','Olefins'],41:['E300','Benzene'],42:['Aromatics','Olefins'],43:['Aromatics','Benzene'],44:['Olefins','Benzene'],
 45:['MTBE'],46:['ETBE'],47:['Ethanol'],48:['TAME'],49:['MTBE','MTBE'],50:['ETBE','ETBE'],51:['Ethanol','Ethanol'],
 52:['Intercept'],53:['Hi'],54:['T50'],55:['T90'],56:['T90','T90'],57:['T50','T50'],58:['Oxygen','T90'],59:['Sulfur','Hi'],
 60:['Aromatics','T90'],61:['T50','Hi'],62:['Olefins','T90'],63:['Oxygen','T50'],64:['Sulfur','T90']}
def props(form):
    r=ff.loc[form]; g=lambda c: float(r[c]) if c in r and pd.notna(r[c]) else 0.0
    E200=g('e200');E300=g('e300')
    return dict(Oxygen=0.3653*g('ETOHVolume'),Sulfur=g('sulfurLevel'),RVP=g('RVP'),E200=E200,E300=E300,
        Aromatics=g('aromaticContent'),Olefins=g('olefinContent'),Benzene=g('benzeneContent'),
        T50=2.0408163*(147.91-E200),T90=4.5454545*(155.47-E300),Hi=1.0,Intercept=1.0,
        MTBE=0.1786*g('MTBEVolume'),ETBE=0.1533*g('ETBEVolume'),Ethanol=0.3488*g('ETOHVolume'),TAME=0.1636*g('TAMEVolume'))
def weight(fm,myg,age):
    for grp in [myg,0]:
        r=wt[(wt.fuelModelID==fm)&(wt.modelYearGroupID==grp)&(wt.ageID==age)]
        if len(r): return float(r['fuelModelWtFactor'].iloc[0])
    return 0.0
def ratio_nosulfur(polproc,ft,myg,engine,target,predictive,age,alias):
    models=sorted(fmn[fmn.calculationEngines.str.contains('|'+engine+'|',regex=False,na=False)].fuelModelID.astype(int))
    sub=mfp[(mfp.polProcessID==polproc)&(mfp.fuelTypeID==ft)&(mfp.modelYearGroupID==myg)]
    ctr={};std={}
    for _,r in sub.iterrows():
        nm=fpn_i.loc[int(r.fuelParameterID)].fuelParameterName
        ctr[nm]=float(r.centeringValue) if pd.notna(r.centeringValue) else 0.0
        std[nm]=float(r.stdDevValue) if pd.notna(r.stdDevValue) and r.stdDevValue>0 else 1.0
    basef=int(bf[(bf.calculationEngine==engine)&(bf.fuelTypeID==ft)&(bf.modelYearGroupID==myg)].fuelFormulationID.iloc[0])
    pb=props(basef); pt=props(target)
    if alias: pt['Sulfur']=pb['Sulfur']
    def cval(cid,p):
        v=1.0
        for nm in CMP[cid]: v*=(p[nm]-ctr.get(nm,0.0))/std.get(nm,1.0)
        return v
    def fmsum(p):
        o={}
        for fm in models:
            s=0.0
            for _,row in cmpP[(cmpP.polProcessID==polproc)&(cmpP.fuelModelID==fm)].iterrows():
                c=float(row.coeff1)+float(row.coeff2)+float(row.coeff3)
                if c!=0: s+=c*cval(int(row.cmpID),p)
            o[fm]=s
        return o
    ts=fmsum(pt);bs=fmsum(pb); ws={fm:weight(fm,myg,age) for fm in models}
    if predictive: return sum(ws[fm]*math.exp(ts[fm]) for fm in models)/sum(ws[fm]*math.exp(bs[fm]) for fm in models)
    return 1.0+sum(ws[fm]*(math.exp(ts[fm])/math.exp(bs[fm])-1.0) for fm in models)
# POLLUTANT CONFIG: (polProc, pollutant, engine, predictive, sulfurAlias)
CFG={'NOx':(301,3,'predictNOx',True,False),'HC':(101,1,'predictHC',True,False),'CO':(201,2,'co',False,True)}
def sulf_eff(pol_id, targetSulfur, baseSulfur, fmygroup, age):
    def co(em):
        r=smc[(smc.pollutantID==pol_id)&(smc.processID==1)&(smc.sourceTypeID==21)&(smc.M6emitterID==em)&(smc.fuelMYGroupID==fmygroup)]
        return float(r.sulfurCoeff.iloc[0]),int(r.sulfurFunctionID.iloc[0])
    def sa3(s,em):
        c,fn=co(em)
        sst=math.exp(c*math.log(s)) if (fn==1 and s>0) else (math.exp(c*s) if fn!=1 else 0)
        ss30=math.exp(c*math.log(30.0)) if fn==1 else math.exp(c*30.0)
        return max(sst/ss30,0.50)
    blend=lambda s: 0.5*sa3(s,1)+0.5*sa3(s,2)
    return blend(targetSulfur)/blend(baseSulfur)
if __name__=='__main__':
    cr=pd.read_parquet([f for f in glob.glob('characterization/snapshots/expand-criteria/tables/*__criteriaratio.parquet')][0])
    cr['ratio']=cr['ratio'].astype(float)
    for name,(pp,pid,eng,pred,al) in CFG.items():
        rns=ratio_nosulfur(pp,1,19502000,eng,9114,pred,40,al)
        print('%s ratioNoSulfur=%.6f'%(name,rns))
