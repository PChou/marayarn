package com.eoi.marayarn;

import org.apache.commons.cli.Option;

import static org.apache.commons.cli.Option.UNINITIALIZED;

/**
 * since apache common-cli >= 1.3 provide Option.Builder.
 * but before that OptionBuilder dose not actually work as Builder pattern
 * copy the Option.Builder to here to keep compatibility
 */
public final class OptionBuilder {
    /** the name of the option */
    private final String opt;

    /** description of the option */
    private String description;

    /** the long representation of the option */
    private String longOpt;

    /** the name of the argument for this option */
    private String argName;

    /** specifies whether this option is required to be present */
    private boolean required;

    /** specifies whether the argument value of this Option is optional */
    private boolean optionalArg;

    /** the number of argument values this option can have */
    private int numberOfArgs = UNINITIALIZED;

    /** the type of this Option */
    private Class<?> type = String.class;

    /** the character that is the value separator */
    private char valuesep;

    /**
     * Constructs a new <code>Builder</code> with the minimum
     * required parameters for an <code>Option</code> instance.
     *
     * @param opt short representation of the option
     * @throws IllegalArgumentException if there are any non valid Option characters in {@code opt}
     */
    public OptionBuilder(final String opt) throws IllegalArgumentException
    {
        this.opt = opt;
    }

    /**
     * Sets the display name for the argument value.
     *
     * @param argName the display name for the argument value.
     * @return this builder, to allow method chaining
     */
    public OptionBuilder argName(final String argName)
    {
        this.argName = argName;
        return this;
    }

    /**
     * Sets the description for this option.
     *
     * @param description the description of the option.
     * @return this builder, to allow method chaining
     */
    public OptionBuilder desc(final String description)
    {
        this.description = description;
        return this;
    }

    /**
     * Sets the long name of the Option.
     *
     * @param longOpt the long name of the Option
     * @return this builder, to allow method chaining
     */
    public OptionBuilder longOpt(final String longOpt)
    {
        this.longOpt = longOpt;
        return this;
    }

    /**
     * Sets the number of argument values the Option can take.
     *
     * @param numberOfArgs the number of argument values
     * @return this builder, to allow method chaining
     */
    public OptionBuilder numberOfArgs(final int numberOfArgs)
    {
        this.numberOfArgs = numberOfArgs;
        return this;
    }

    /**
     * Sets whether the Option can have an optional argument.
     *
     * @param isOptional specifies whether the Option can have
     * an optional argument.
     * @return this builder, to allow method chaining
     */
    public OptionBuilder optionalArg(final boolean isOptional)
    {
        this.optionalArg = isOptional;
        return this;
    }

    /**
     * Marks this Option as required.
     *
     * @return this builder, to allow method chaining
     */
    public OptionBuilder required()
    {
        return required(true);
    }

    /**
     * Sets whether the Option is mandatory.
     *
     * @param required specifies whether the Option is mandatory
     * @return this builder, to allow method chaining
     */
    public OptionBuilder required(final boolean required)
    {
        this.required = required;
        return this;
    }

    /**
     * Sets the type of the Option.
     *
     * @param type the type of the Option
     * @return this builder, to allow method chaining
     */
    public OptionBuilder type(final Class<?> type)
    {
        this.type = type;
        return this;
    }

    /**
     * The Option will use '=' as a means to separate argument value.
     *
     * @return this builder, to allow method chaining
     */
    public OptionBuilder valueSeparator()
    {
        return valueSeparator('=');
    }

    /**
     * The Option will use <code>sep</code> as a means to
     * separate argument values.
     * <p>
     * <b>Example:</b>
     * <pre>
     * Option opt = Option.builder("D").hasArgs()
     *                                 .valueSeparator('=')
     *                                 .build();
     * Options options = new Options();
     * options.addOption(opt);
     * String[] args = {"-Dkey=value"};
     * CommandLineParser parser = new DefaultParser();
     * CommandLine line = parser.parse(options, args);
     * String propertyName = line.getOptionValues("D")[0];  // will be "key"
     * String propertyValue = line.getOptionValues("D")[1]; // will be "value"
     * </pre>
     *
     * @param sep The value separator.
     * @return this builder, to allow method chaining
     */
    public OptionBuilder valueSeparator(final char sep)
    {
        valuesep = sep;
        return this;
    }

    /**
     * Indicates that the Option will require an argument.
     *
     * @return this builder, to allow method chaining
     */
    public OptionBuilder hasArg()
    {
        return hasArg(true);
    }

    /**
     * Indicates if the Option has an argument or not.
     *
     * @param hasArg specifies whether the Option takes an argument or not
     * @return this builder, to allow method chaining
     */
    public OptionBuilder hasArg(final boolean hasArg)
    {
        // set to UNINITIALIZED when no arg is specified to be compatible with OptionBuilder
        numberOfArgs = hasArg ? 1 : UNINITIALIZED;
        return this;
    }

    /**
     * Indicates that the Option can have unlimited argument values.
     *
     * @return this builder, to allow method chaining
     */
    public OptionBuilder hasArgs()
    {
        numberOfArgs = Option.UNLIMITED_VALUES;
        return this;
    }

    /**
     * Constructs an Option with the values declared by this {@link OptionBuilder}.
     *
     * @return the new {@link Option}
     * @throws IllegalArgumentException if neither {@code opt} or {@code longOpt} has been set
     */
    public Option build()
    {
        if (opt == null && longOpt == null)
        {
            throw new IllegalArgumentException("Either opt or longOpt must be specified");
        }
        Option option = new Option(this.opt, this.description);
        option.setArgName(this.argName);
        option.setLongOpt(this.longOpt);
        option.setOptionalArg(this.optionalArg);
        option.setRequired(this.required);
        option.setType(this.type);
        option.setValueSeparator(this.valuesep);
        option.setArgs(this.numberOfArgs);
        return option;
    }
}
