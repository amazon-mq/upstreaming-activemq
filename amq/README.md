
## Setup

### Java

1. Install Java 11 (I recommend `jenv`)
```bash
        git clone https://github.com/jenv/jenv.git ~/.jenv
        eval "$(~/.jenv/bin/jenv init -)"
        jenv add /usr/local/Cellar/openjdk/`ls /usr/local/Cellar/openjdk/`/libexec/openjdk.jdk/Contents/Home
        jenv enable-plugin export
        jenv enable-plugin maven
        jenv enable-plugin gradle
```

### IDE Setup

1. `mvn clean install -Dmaven.test.skip=true`
2. `mvn idea:idea`
3. Menu (shift+shift) > Edit configurations and add the configs in the `.run` Folder
4. Project Structure > Make sure SDK and language level are java 11 (https://intellij-support.jetbrains.com/hc/en-us/community/posts/360010626499/comments/4406957997586)
5. https://stackoverflow.com/a/17194980/14709424 so you don't get accidental formatting diffs

## Running

0. Copy all the `sample-*` files in the directory to `my-*` to have your own version of them.
1. Run the `run-built-activemq.sh` script.
2. Once the terminal runs the broker, you can run the script to stop it (i.e. `./amq/run-built-activemq.sh stop`) 
