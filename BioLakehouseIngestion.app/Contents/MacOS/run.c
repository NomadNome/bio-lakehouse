#include <stdlib.h>
#include <unistd.h>

int main(void) {
    setenv("PYTHONUNBUFFERED", "1", 1);
    setenv("PATH", "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin", 1);
    setenv("HOME", "/Users/nomathadejenkins", 1);
    char *args[] = {"/bin/bash", "/Users/nomathadejenkins/Desktop/Bio Lakehouse/run_daily_ingestion.sh", NULL};
    return execv("/bin/bash", args);
}
