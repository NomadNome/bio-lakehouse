#!/bin/bash
# Creates a Folder Action that watches ~/Downloads/ and moves bio exports to inbox/
set -eu

INBOX="$HOME/Desktop/Bio Lakehouse/inbox"
WORKFLOW_DIR="$HOME/Library/Workflows/Applications/Folder Actions"
mkdir -p "$WORKFLOW_DIR" "$INBOX"

# Create the AppleScript-based Folder Action
cat > /tmp/bio_inbox_mover.applescript << 'APPLESCRIPT'
on adding folder items to theFolder after receiving theFiles
    set inboxPath to (POSIX path of (path to home folder)) & "Desktop/Bio Lakehouse/inbox/"
    repeat with aFile in theFiles
        set fileName to name of (info for aFile)
        if fileName starts with "export" and fileName ends with ".zip" then
            do shell script "mv " & quoted form of (POSIX path of aFile) & " " & quoted form of inboxPath
        else if fileName starts with "KnownasNoma_workouts" and fileName ends with ".csv" then
            do shell script "mv " & quoted form of (POSIX path of aFile) & " " & quoted form of inboxPath
        else if fileName starts with "Nutrition-Summary" and fileName ends with ".csv" then
            do shell script "mv " & quoted form of (POSIX path of aFile) & " " & quoted form of inboxPath
        end if
    end repeat
end adding folder items to
APPLESCRIPT

osacompile -o "$WORKFLOW_DIR/Bio Lakehouse Inbox Mover.workflow" /tmp/bio_inbox_mover.applescript
rm /tmp/bio_inbox_mover.applescript

echo "Folder Action installed at: $WORKFLOW_DIR/Bio Lakehouse Inbox Mover.workflow"
echo ""
echo "Enable it:"
echo "  1. Right-click ~/Downloads in Finder"
echo "  2. Services > Folder Actions Setup..."
echo "  3. Attach 'Bio Lakehouse Inbox Mover' to Downloads"
