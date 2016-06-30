package com.littlersmall.rabbitmqaccess.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by littlersmall on 16/5/10.
 */
//统一返回值,可描述失败细节
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DetailRes {
    boolean isSuccess;
    String errMsg;
}
