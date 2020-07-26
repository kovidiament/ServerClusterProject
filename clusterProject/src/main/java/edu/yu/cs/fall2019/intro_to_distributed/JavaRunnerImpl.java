package edu.yu.cs.fall2019.intro_to_distributed;



import javax.tools.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaRunnerImpl {

    Path codePath;

    public JavaRunnerImpl() throws IOException {
        this.codePath = Files.createTempDirectory("temp");
    }
    public JavaRunnerImpl(Path path) throws IOException {
        if(Files.exists(path))
        {
            this.codePath = path;
        }
        else{

                this.codePath = Files.createDirectories(path);
        }
    }

    public String compileAndRun(InputStream in) throws IllegalArgumentException, IOException
            /*
            Reads in source code, parses for class and package name, saves to a file in
            designated path, and sends to runStuff() for compilation and running.
            Name provided to runStuff() will be either the class name, or, if there is a package, the file will be saved in that package,
            and the name given to runstuff will be the binary name with package.
             */
    {
        String packageString = "";
        String packagePath = "";
        String className="";
        File[] files = new File[1];
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        Path tempFile = Files.createTempDirectory("").resolve(UUID.randomUUID().toString() + ".tmp");
        Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        String temp = new String(Files.readAllBytes(tempFile));

         Pattern classPattern = Pattern.compile("(?<=\\n|\\A)(?:public\\s)?(class|interface|enum)\\s([^\\n\\s]*)");
         Matcher classMatcher = classPattern.matcher(temp);
         if(classMatcher.find())
         {
             className = classMatcher.group(2);
         }
         Pattern packagePattern = Pattern.compile("package\\s+([a-zA_Z_][\\.\\w]*);");
         Matcher packageMatcher = packagePattern.matcher(temp);
         if(packageMatcher.find())
         {
             packageString = packageMatcher.group(1)+".";

             //packagePath gets set by replacing . with file seperator and adding seperator at end.
             packagePath = packageString.replace('.', File.separator.charAt(0));
             packagePath = packagePath+File.separator;
         }

        File dirStructure = new File(codePath.toString()+File.separator+packagePath);
         dirStructure.mkdirs();
         File file = new File(codePath.toString()+File.separator+packagePath+className+".java");
         file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(temp);
        writer.close();
        files[0] = file;
        className = packageString+className;
        return runStuff(files, className);

    }

    private  String runStuff(File[] files, String name) throws IOException
            /*
           returns a string of system out and err. Compilation errors or lack of a run() method will result in a message thrown
           in an IllegalArgumentException, through the compileAndRun method. Such an exception being thrown indicates an error 400 code should be set.
             */
    {
        String result = "";
        String cause = "";
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
        Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(files));
        if(compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits).call())
        {
               fileManager.close();
                Class theClass;
                Method run;
               /*ClassLoader classLoader = JavaRunnerImpl.class.getClassLoader();*/
               URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] {this.codePath.toUri().toURL()});

                try{
                    theClass = classLoader.loadClass(name);
                }
                catch(ClassNotFoundException e)
                {
                    throw new IllegalArgumentException("class not found! something went wrong.\n");
                }

               try{
                   run = theClass.getMethod("run");
               }
               catch (NoSuchMethodException e)
               {
                   throw new IllegalArgumentException("Class "+name+" does not contain a run() method.\n");
               }
                String sysOut;
                String sysErr;
                synchronized (JavaRunnerImpl.class)
                {
                    ByteArrayOutputStream stream = new ByteArrayOutputStream();
                    PrintStream ps = new PrintStream(stream);
                    PrintStream originalPrintStream = System.out;
                    System.setOut(ps);


                    ByteArrayOutputStream errStream = new ByteArrayOutputStream();
                    PrintStream ps2 = new PrintStream(errStream);
                    PrintStream originalErrorStream = System.err;
                    System.setErr(ps2);

                    try{
                        run.invoke(theClass.getConstructor().newInstance());
                    }
                    catch (Exception e)
                    {
                        Throwable exc = e.getCause();
                        cause = exc.toString();
                    }

                    sysOut = new String(stream.toByteArray());
                    sysErr = new String(errStream.toByteArray());
                    //sysErr = cause;
                    System.setOut(originalPrintStream);
                    System.setErr(originalErrorStream);


                }

            result = "System.err:\n["+sysErr +cause+"]\nSystem.out:\n["+sysOut+"]";
        }

       else{
            String problems = "";
            for ( Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics())
            {
                problems = problems + "Error on line "+ diagnostic.getLineNumber()+ "in column "+diagnostic.getColumnNumber()+ " in "+diagnostic.getSource().toUri()+"";
            }
               throw new IllegalArgumentException(problems);


        }
        return result;
    }

}
