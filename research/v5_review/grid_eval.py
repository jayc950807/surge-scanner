import numpy as np
def evalp(P,tp,sl,hold):
    ep=P[:,0,0]; n=len(ep); r=np.full(n,np.nan); done=np.zeros(n,bool)
    tpp=ep*(1+tp) if tp else np.full(n,np.inf); slp=ep*(1+sl) if sl else np.full(n,-np.inf)
    for k in range(hold):
        o,h,l,c=P[:,k,0],P[:,k,1],P[:,k,2],P[:,k,3]
        if k>0:
            g=~done&(o<=slp); r[g]=o[g]/ep[g]-1; done|=g
            g=~done&(o>=tpp); r[g]=o[g]/ep[g]-1; done|=g
        th=h>=tpp; sh=l<=slp
        g=~done&th&sh; r[g]=sl; done|=g
        g=~done&th; r[g]=tp; done|=g
        g=~done&sh; r[g]=sl; done|=g
        if k==hold-1:
            g=~done; r[g]=c[g]/ep[g]-1; done|=g
    return r
