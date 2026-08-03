package io.mosip.commons.khazana.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ObjectStoreReference {

    private String account;
    private String container;
    private String source;
    private String process;
    private String objectName;
}
