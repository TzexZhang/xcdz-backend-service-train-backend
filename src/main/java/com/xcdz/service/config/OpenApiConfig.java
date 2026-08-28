package com.xcdz.service.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI 3 / Knife4j 文档元信息配置
 * <p>
 * 访问地址：
 * - Knife4j UI: http://localhost:9900/doc.html
 * - Swagger UI: http://localhost:9900/swagger-ui.html
 * - OpenAPI JSON: http://localhost:9900/v3/api-docs
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("xcdz-backend-service API 文档")
                        .version("1.0")
                        .description("短波天线类型管理后端接口")
                        .contact(new Contact()
                                .name("ztz"))
                        .license(new License()
                                .name("Apache 2.0")));
    }
}
