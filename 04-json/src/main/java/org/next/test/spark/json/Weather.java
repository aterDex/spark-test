package org.next.test.spark.json;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;

@Data
public class Weather implements Serializable {

    static final long serialVersionUID = 42L;

    private Double avgwindspeed;
    private String blowingsnow;
    private LocalDate date;
    private String drizzle;
    private String dust;
    private Double fastest2minwinddir;
    private Double fastest2minwindspeed;
    private Double fastest5secwinddir;
    private Double fastest5secwindspeed;
    private String fog;
    private String fogground;
    private String fogheavy;
    private String freezingfog;
    private String freezingrain;
    private String glaze;
    private String hail;
    private String highwind;
    private String ice;
    private String mist;
    private Double precipitation;
    private String rain;
    private String smokehaze;
    private String snow;
    private Double snowdepth;
    private Double snowfall;
    private Double temperaturemax;
    private Double temperaturemin;
    private String thunder;
}
