package com.iflytek.kuyin.cold;

import java.io.Serializable;

/**
 * Created by jjhu on 2017/12/20.
 */
public class EsRing implements Serializable {
    private static final String INDEX = "kuyin";
    private static final String TYPE = "ring";
    private static final String NAME = "name";
    private static final String SINGER = "singer";
    private static final String TAGS = "tags";
    private static final String CTM = "ctm";
    private static final String FIRE = "fire";

    public static String getINDEX() {
        return INDEX;
    }

    public static String getTYPE() {
        return TYPE;
    }

    public static String getNAME() {
        return NAME;
    }

    public static String getSINGER() {
        return SINGER;
    }

    public static String getTAGS() {
        return TAGS;
    }

    public static String getCTM() {
        return CTM;
    }

    public static String getFIRE() {
        return FIRE;
    }
}