package com.eoi.marayarn.web.entity.body;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by wenbo.gong on 2020/10/10
 */
@Getter
@Setter
@NoArgsConstructor
public class UploadReq {
    private String url;
    private String destDir;
    private String fileName;
}
