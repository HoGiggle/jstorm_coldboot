package com.iflytek.kuyin.cold;

import java.io.Serializable;

/**
 * Created by jjhu on 2017/12/18.
 */
public class CommonKeys implements Serializable{
    //log fields
    private static final String UID = "uid";
    private static final String AUDIOS = "audios";
    private static final String SINGER = "d_singername";
    private static final String SONG = "d_songname";

    //spout bolt output fields
    private static final String RECALLS = "recalls";



    public static String getUID() {
        return UID;
    }

    public static String getAUDIOS() {
        return AUDIOS;
    }

    public static String getSINGER() {
        return SINGER;
    }

    public static String getSONG() {
        return SONG;
    }

    public static String getRECALLS() {
        return RECALLS;
    }
}

class Music implements Serializable{
    private String name;
    private String[] singer;

    public Music(String name, String[] singer) {
        this.name = name;
        this.singer = singer;
    }

    public String getName() {
        return name;
    }

    public String[] getSinger() {
        return singer;
    }
}
