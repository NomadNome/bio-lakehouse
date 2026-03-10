#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

int main(void) {
    setenv("PYTHONUNBUFFERED", "1", 1);
    setenv("PATH", "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin", 1);

    const char *home = getenv("HOME");
    if (!home) return 1;

    char script[512];
    snprintf(script, sizeof(script), "%s/Desktop/Bio Lakehouse/run_daily_ingestion.sh", home);

    char *args[] = {"/bin/bash", script, NULL};
    return execv("/bin/bash", args);
}
