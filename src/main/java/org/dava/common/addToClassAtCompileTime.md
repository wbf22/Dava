To achieve what Lombok does, you'd need to write an annotation processor. An annotation processor allows you to hook into the compilation process and generate code based on annotations. Writing a full-fledged annotation processor involves understanding the Java Annotation Processing API and the Java Compiler API.

Here is a simplified overview of how you could create an annotation processor to generate methods at compile time:

1. **Create an Annotation:** Define a custom annotation that will be used to trigger the generation of methods. For example:

```java
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
public @interface CustomAnnotation {
}
```

2. **Write the Annotation Processor:** Implement the `AbstractProcessor` class to process your custom annotation. This involves overriding the `process()` method to generate the required methods.

```java
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.TypeElement;
import java.util.Set;

public class CustomProcessor extends AbstractProcessor {

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        // Add logic to generate methods based on the annotations
        return true;
    }
}
```

3. **Register the Processor:** Create a file named `javax.annotation.processing.Processor` in the `META-INF/services` directory of your resources folder. In this file, add the fully qualified name of your processor.

4. **Compile and Run the Processor:** Use the `javac` command to compile your code and run the annotation processor. The processor will be triggered when the custom annotation is detected during compilation.

Writing a custom annotation processor is a non-trivial task, and it involves a deep understanding of the Java Compiler API and the Annotation Processing API. For a simple use case, Lombok provides a robust and easy-to-use solution.


Certainly, you can modify the custom processor to add a static member variable to the class with the annotation. Here's an example of how to achieve this:

```java
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

public class CustomProcessor extends AbstractProcessor {

    private Elements elementUtils;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        elementUtils = processingEnv.getElementUtils();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(CustomAnnotation.class)) {
            // Check if it is a class
            if (annotatedElement instanceof TypeElement) {
                TypeElement typeElement = (TypeElement) annotatedElement;
                String className = typeElement.getSimpleName().toString();
                String packageName = elementUtils.getPackageOf(typeElement).toString();

                try {
                    // Generate the source code
                    generateStaticField(packageName, className);
                } catch (IOException e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.toString(), annotatedElement);
                }
            }
        }
        return true;
    }

    private void generateStaticField(String packageName, String className) throws IOException {
        // Create the source file
        PrintWriter writer = new PrintWriter(processingEnv.getFiler().createSourceFile(packageName + "." + className).openWriter());

        // Add the static field to the class
        writer.println("package " + packageName + ";");
        writer.println();
        writer.println("public class " + className + " {");
        writer.println("    public static String staticField = \"Static Field Value\";");
        writer.println("}");

        writer.close();
    }
}
```

In this example, the `generateStaticField` method is used to create a new source file and add a static field to the class specified by the annotation. Make sure to modify the logic according to your specific requirements.