package t4

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

func TestAWSConfigOptionsRequireT4CredentialsOrProfile(t *testing.T) {
	_, err := awsConfigOptions("", "")
	if err == nil {
		t.Fatal("awsConfigOptions returned nil error")
	}
	if !strings.Contains(err.Error(), "T4_S3_PROFILE") {
		t.Fatalf("awsConfigOptions error = %q, want mention of T4_S3_PROFILE", err)
	}
}

func TestAWSConfigOptionsUseT4Credentials(t *testing.T) {
	t.Setenv("T4_S3_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("T4_S3_SECRET_ACCESS_KEY", "test-secret-key")

	opts, err := awsConfigOptions("", "")
	if err != nil {
		t.Fatalf("awsConfigOptions returned error: %v", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		t.Fatalf("LoadDefaultConfig returned error: %v", err)
	}

	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve returned error: %v", err)
	}
	if creds.AccessKeyID != "test-access-key" {
		t.Fatalf("AccessKeyID = %q, want %q", creds.AccessKeyID, "test-access-key")
	}
	if creds.SecretAccessKey != "test-secret-key" {
		t.Fatalf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, "test-secret-key")
	}
}

func TestAWSConfigOptionsAllowAWSChainWithT4Profile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(configPath, []byte("[profile test-profile]\nregion = us-west-2\n"), 0600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", configPath)
	t.Setenv("T4_S3_PROFILE", "test-profile")

	opts, err := awsConfigOptions("", "")
	if err != nil {
		t.Fatalf("awsConfigOptions returned error: %v", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		t.Fatalf("LoadDefaultConfig returned error: %v", err)
	}
	if cfg.Credentials == nil {
		t.Fatal("Credentials provider is nil")
	}
}

func TestAWSConfigOptionsRequireCompleteT4Credentials(t *testing.T) {
	tests := []struct {
		name            string
		accessKeyID     string
		secretAccessKey string
	}{
		{
			name:        "missing secret access key",
			accessKeyID: "test-access-key",
		},
		{
			name:            "missing access key ID",
			secretAccessKey: "test-secret-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("T4_S3_ACCESS_KEY_ID", tt.accessKeyID)
			t.Setenv("T4_S3_SECRET_ACCESS_KEY", tt.secretAccessKey)

			if _, err := awsConfigOptions("", ""); err == nil {
				t.Fatal("awsConfigOptions returned nil error")
			}
		})
	}
}
