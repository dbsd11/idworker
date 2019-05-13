package com.imadcn.framework.idworker.algorithm;

/**
 * @author Created by diaobisong on 2019/2/19.
 */
public interface IdGene {

    public long nextId();

    public long[] nextId(int size);

    public int getSize();
}
