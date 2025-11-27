package io.xrex.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/internal", produces = "application/json;charset=utf-8")
public class VersionController {

    // for version information
    @Value("${IMAGE_VERSION:unset}")
    private String imageVersion;

    @Value("${RELEASE_VERSION:unset}")
    private String releaseVersion;

    @Value("${GIT_COMMIT_ID:unset}")
    private String gitCommitId;

    @Value("${GIT_BRANCH:unset}")
    private String gitBranch;

    // for runtime environment
    @Value("${APP_ENV:unset}")
    private String appEnv;

    @Value("${CONFIG_HASH:unset}")
    private String configHash;

    @Value("${CONFIG_COMMIT_ID:unset}")
    private String configCommitId;

    @Value("${APP_INSTITUTION:unset}")
    private String appInstitution;

    @GetMapping(value = "/version")
    public RestApiResponse<VersionResponse> getVersion() {

        VersionResponse data = VersionResponse.builder().imageVersion(imageVersion)
                .releaseVersion(releaseVersion)
                .gitCommitId(gitCommitId)
                .gitBranch(gitBranch).build();

        return RestApiResponse.ok(data);
    }

    @GetMapping(value = "/env")
    public RestApiResponse<EnvResponse> getEnvironment() {

        EnvResponse data = EnvResponse.builder().appEnv(appEnv)
                .appInstitution(appInstitution)
                .configHash(configHash)
                .configCommitId(configCommitId).build();

        return RestApiResponse.ok(data);
    }

    @Data
    @Builder
    static class VersionResponse {
        @JsonProperty("image_version")
        private String imageVersion;

        @JsonProperty("release_version")
        private String releaseVersion;

        @JsonProperty("git_commit_id")
        private String gitCommitId;

        @JsonProperty("git_branch")
        private String gitBranch;
    }

    @Data
    @Builder
    static class EnvResponse {
        @JsonProperty("app_env")
        private String appEnv;

        @JsonProperty("app_institution")
        private String appInstitution;

        @JsonProperty("config_hash")
        private String configHash;

        @JsonProperty("config_commit_id")
        private String configCommitId;

    }
}
