# EventBus

EventBus for **WebMorph** ecosystem or external Spring-based applications. Incredibly easy to use. Subscribe, transform, or cancel events with full control over the reactive flow — all with minimal overhead.

<p align="center">
<a href="https://github.com/web-morph/eventbus?tab=LGPL-3.0-1-ov-file"><img alt="License" src="https://img.shields.io/github/license/web-morph/eventbus"></a>
<a href="https://docs.gradle.org/8.14/release-notes.html"><img src="https://img.shields.io/badge/Gradle-8.14-brightgreen.svg?colorB=469C00&logo=gradle"></a>
<a href="https://repo.billmarssoft.com/api/maven/latest/file/releases/com/github/webmorph/eventbus?extension=jar" target="_blank"><img alt="Download" src="https://repo.billmarssoft.com/api/badge/latest/releases/com/github/webmorph/eventbus"></a>
<a href="https://repo.billmarssoft.com/javadoc/releases/com/github/webmorph/eventbus/latest" target="_blank"><img alt="Download" src="https://img.shields.io/badge/javadoc-latest-red"></a>
</p>

---

## ⚙️ Requirements

* Java 17 or above

## 📦 Installation

⚙️ Gradle (Kotlin DSL – build.gradle.kts)

```kts
repositories {
    maven("https://repo.billmarssoft.com/public/")
}

dependencies {
    implementation("com.github.webmorph:eventbus:<version>")
}
```

⚙️ Gradle (Groovy DSL – build.gradle)

```groovy
repositories {
    maven {
        url 'https://repo.billmarssoft.com/public/'
    }
}

dependencies {
    implementation "com.github.webmorph:eventbus:<version>"
}
```

## 🧰 Usage

### 1. Define Your Event
Create a custom event by extending the base ```Event``` class.
```java
public class UserLoginEvent extends Event {
    private final String username;
    private boolean status = false;

    public UserLoginEvent(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }
    
    public void setStatus(boolean status) {
        this.status = status;
    }
    
    public boolean getStatus() {
        return this.status;
    }
}
```

### 2. Register a Handler (Programmatically)
You can subscribe to events using the ```EventBus#on``` method:
```java
static {
    eventBus.on(UserLoginEvent.class, event -> {
        System.out.println("User logged in: " + event.getUsername());
    });
}
```
You can also specify priority and cancellation behavior:
```java
static {
    eventBus.on(UserLoginEvent.class, EventPriority.HIGH, false /*force flag*/, event -> {
        System.out.println("High priority handler for: " + event.getUsername());
    });
}
```

### 3. Declarative Handler Registration
To register handlers via annotations, implement the ```Listener``` interface and annotate your method with ```@EventHandler```.
```java
@Component
public class LoginListener implements Listener {

    @EventHandler(
            priority = EventPriority.NORMAL, // Events are propagated for each priority level (from LOWEST to HIGHEST) in sequence.
            force = false // Always true to always invoke the handler; false to skip if cancelled
    )
    public void onLogin(UserLoginEvent event) {
        System.out.println("Annotated handler: " + event.getUsername());
        event.setStatus(true); // you can modify data
    }
}
```

### 4. Dispatch an Event
Use ```dispatchEvent``` to publish an event:
```java
static {
    UserLoginEvent event = eventBus.dispatchEvent(new UserLoginEvent("alice"));
    boolean status = event.getStatus();
    System.out.println(status);
}
```

### 5. Cancelable Events
If your event extends ```CancelableEvent```, handlers can cancel its propagation:
```java
public class MessageSendEvent extends CancelableEvent {
    private final String message;

    public MessageSendEvent(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
```
Then in a handler:
```java
@EventHandler(priority = EventPriority.NORMAL)
public void checkProfanity(MessageSendEvent event) {
    if (event.getMessage().contains("badword")) {
        event.setCanceled(true);
    }
}

@EventHandler(priority = EventPriority.HIGHT, force = true /*Force true allow handle even if event was canceled in previous handler */)
public void checkProfanityForce(MessageSendEvent event) {
    System.out.println("User make attempt to send message: " + event.getMessage());
}
```

## ✅ Notes
* You can register handlers globally or via Spring beans.
* Events are propagated for each priority level (from ```LOWEST``` to ```HIGHEST```) in sequence.
* Backpressure is handled internally via Reactor's ```Flux``` and ```Sinks```.

------

# 🛠️ Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## 🧍 Author

### [CKATEPTb](https://github.com/CKATEPTb), [fakeivchenko](https://github.com/fakeivchenko)
