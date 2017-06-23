
####################
# Tab completion file to allow for easy use of this tool with the command-line (bash/zsh)
####################

<#include "bash-completion.macros.ftl"/>

####################################################################################################

# High-level caller/dispatch script information:

CALLER_SCRIPT_NAME="gatk-launch"

CS_ALL_LEGAL_ARGUMENTS=(--help --list --sparkRunner --dryRun --javaOptions --conf --driver-memory --driver-cores --executor-memory --executor-cores --num-executors)
CS_NORMAL_COMPLETION_ARGUMENTS=(--help --list --sparkRunner --dryRun --javaOptions --conf --driver-memory --driver-cores --executor-memory --executor-cores --num-executors)
CS_ALL_ARGUMENT_VALUE_TYPES=("null" "null" "String" "null" "String" "file" "int" "int" "int" "int" "int" )
CS_MUTUALLY_EXCLUSIVE_ARGS=("--help;list,sparkRunner,dryRun,javaOptions,conf,driver-memory,driver-cores,executor-memory,executor-cores,num-executors" "--list;help,sparkRunner,dryRun,javaOptions,conf,driver-memory,driver-cores,executor-memory,executor-cores,num-executors" )
CS_SYNONYMOUS_ARGS=("--help;-h")
CS_MIN_OCCURRENCES=(0 0 0 0 0 0 0 0 0 0 0)
CS_MAX_OCCURRENCES=(1 1 1 1 1 1 1 1 1 1 1)

####################################################################################################

# Get the name of the tool that we're currently trying to call
_getToolName()
{
    echo $( echo "${r"${COMP_LINE}"}" | sed "s#^.*${r"${CALLER_SCRIPT_NAME}"}[ \\t]*\\([a-zA-Z0-9]*\\)[ \\t]*.*#\\1#g" )
}

# Gets how many dependent arguments we have left to fill
_getDependentArgumentCount()
{
    local depArgCount=0

    for word in ${r"${COMP_LINE}"} ; do
        for depArg in ${r"${DEPENDENT_ARGUMENTS[@]}"} ; do
            if [[ "${r"${word}"}" == "${r"${depArg}"}" ]] ; then
                $((depArgCount++))
            fi
        done
    done

    echo ${r"$depArgCount"}
}

# Resolves the given argument name to its long (normal) name
_resolveVarName()
{
    local argName=$1
    if [[ "${r"${SYNONYMOUS_ARGS[@]}"}" == *"${r"${argName}"}"* ]] ; then
        echo "${r"${SYNONYMOUS_ARGS[@]}"}" | sed -e "s#.* \\([a-zA-Z0-9;,_\\-]*${r"${argName}"}[a-zA-Z0-9,;_\\-]*\\).*#\\1#g" -e 's#;.*##g'
    else
        echo "${r"${argName}"}"
    fi
}

# Checks if we need to complete the VALUE for an argument.
# Prints the index in the given argument list of the corresponding argument whose value we must complete.
# Takes as input 1 positional argument: the name of the last argument given to this script
# Otherwise prints -1
_needToCompleteArgValue()
{
    local resolved=$( _resolveVarName ${r"${prev}"} )

    ${r"for (( i=0 ; i < ${#ALL_LEGAL_ARGUMENTS[@]} ; i++ )) ; do"}
        if [[ "${r"${resolved}"}" == "${r"${ALL_LEGAL_ARGUMENTS[i]}"}" ]] ; then

            # Make sure the argument isn't one that takes no additional value
            # such as a flag.
            if [[ "${r"${ALL_ARGUMENT_VALUE_TYPES[i]}"}" != "null" ]] ; then
                echo "$i"
            else
                echo "-1"
            fi
            return 0
        fi
    done

    echo "-1"
}

# Get the completion word list for the given argument type.
# Prints the completion string to the screen
_getCompletionWordList()
{
    # Normalize the type string so it's easier to deal with:
    local argType=$( echo $1 | tr '[A-Z]' '[a-z]')

    local isNumeric=false
    local isFloating=false

    local completionType=""

    [[ "${r"${argType}"}" == *"file"* ]]      && completionType='-A file'
    [[ "${r"${argType}"}" == *"folder"* ]]    && completionType='-A directory'
    [[ "${r"${argType}"}" == *"directory"* ]] && completionType='-A directory'
    [[ "${r"${argType}"}" == *"boolean"* ]]   && completionType='-W true false'

    [[ "${r"${argType}"}" == "int" ]]         && completionType='-W 0 1 2 3 4 5 6 7 8 9'   && isNumeric=true
    [[ "${r"${argType}"}" == *"[int]"* ]]     && completionType='-W 0 1 2 3 4 5 6 7 8 9'   && isNumeric=true
    [[ "${r"${argType}"}" == "long" ]]        && completionType='-W 0 1 2 3 4 5 6 7 8 9'   && isNumeric=true
    [[ "${r"${argType}"}" == *"[long]"* ]]    && completionType='-W 0 1 2 3 4 5 6 7 8 9'   && isNumeric=true

    [[ "${r"${argType}"}" == "double" ]]      && completionType='-W . 0 1 2 3 4 5 6 7 8 9' && isNumeric=true && isFloating=true
    [[ "${r"${argType}"}" == *"[double]"* ]]  && completionType='-W . 0 1 2 3 4 5 6 7 8 9' && isNumeric=true && isFloating=true
    [[ "${r"${argType}"}" == "float" ]]       && completionType='-W . 0 1 2 3 4 5 6 7 8 9' && isNumeric=true && isFloating=true
    [[ "${r"${argType}"}" == *"[float]"* ]]   && completionType='-W . 0 1 2 3 4 5 6 7 8 9' && isNumeric=true && isFloating=true

    # If we have a number, we need to prepend the current completion to it so that we can continue to tab complete:
    if $isNumeric ; then
        completionType=$( echo ${r"${completionType}"} | sed -e "s#\([0-9]\)#$cur\1#g" )

        # If we're floating point, we need to make sure we don't complete a `.` character
        # if one already exists in our number:
        if $isFloating ; then
            echo "$cur" | grep -o '\.' &> /dev/null
            local r=$?

            [[ $r -eq 0 ]] && completionType=$( echo ${r"${completionType}"} | awk '{$2="" ; print}' )
        fi
    fi

    echo "${r"${completionType}"}"
}

# Function to handle the completion tasks once we have populated our arg variables
# When passed an argument handles the case for the caller script.
_handleArgs()
{
    # Argument offset index is used in the special case where we are past the " -- " delimiter.
    local argOffsetIndex=0

    # We handle the beginning differently if this function was called with an argument
    if [[ $# -eq 0 ]] ; then
        # By the time we're in this function, we know we have the executable name
        # and then the tool name.  Therefore we must subtract two from the arg
        # count:
        local numArgs=$((COMP_CWORD-2))

        # Now we check to see what kind of argument we are on right now
        # We handle each type separately by order of precedence:
        ${r"if [[ ${numArgs} -lt ${NUM_POSITIONAL_ARGUMENTS} ]] ; then"}
            # We must complete a positional argument.
            # Assume that positional arguments are all FILES:
            COMPREPLY=( ${r"$(compgen -A file -- $cur"}) )
            return 0
        fi

        # Dependent arguments must come right after positional arguments
        # We must check to see how many dependent arguments we've gotten so far:
        local numDepArgs=${r"$"}( _getDependentArgumentCount )

        ${r"if [[ $numDepArgs -lt ${#DEPENDENT_ARGUMENTS[@]} ]] ; then"}
            # We must complete a dependent argument next.
            COMPREPLY=( ${r"$(compgen -W '${DEPENDENT_ARGUMENTS[@]}' -- $cur"}) )
            return 0
        fi
    else
        # Get the index of the special delimiter.
        # we ignore everything up to and including it.
        for (( i=0; i < COMP_CWORD ; i++ )) ; do
            if [[ "${r"${COMP_WORDS[i]}"}" == "--" ]] ; then
                let argOffsetIndex=$i+1
            fi
        done
    fi

    # First we must resolve all arguments to their full names
    # This is necessary to save time later because of short argument names / synonyms
    local resolvedArgList=()
    for (( i=argOffsetIndex ; i < COMP_CWORD ; i++ )) ; do
        prevArg=${r"${COMP_WORDS[i]}"}

        # Skip the current word to be completed:
        [[ "${r"${prevArg}"}" == "${r"${cur}"}" ]] && continue

        # Check if this has synonyms:
        if [[ "${r"${SYNONYMOUS_ARGS[@]}"}" == *"${r"${prevArg}"}"* ]] ; then

            local resolvedArg=$( _resolveVarName "${r"${prevArg}"}" )
            ${r"resolvedArgList+=($resolvedArg)"}

        # Make sure this is an argument:
        elif [[ "${r"${ALL_LEGAL_ARGUMENTS[@]}"}" == *"${r"${prevArg}"}"* ]] ; then
            ${r"resolvedArgList+=($prevArg)"}
        fi
    done

    # Check to see if the last thing we typed was a complete argument.
    # If so, we must complete the VALUE for the argument, not the
    # argument itself:
    # Note: This is shorthand for last element in the array:
    local argToComplete=$( _needToCompleteArgValue )

    if [[ $argToComplete -ne -1 ]] ; then
        # We must complete the VALUE for an argument.

        # Get the argument type.
        local valueType=${r"${ALL_ARGUMENT_VALUE_TYPES[argToComplete]}"}

        # Get the correct completion string for the type:
        local completionString=$( _getCompletionWordList "${r"${valueType}"}" )

        if [[ ${r"${#completionString}"} -eq 0 ]] ; then
            # We don't have any information on the type to complete.
            # We use the default SHELL behavior:
            COMPREPLY=()
        else
            # We have a completion option.  Let's plug it in:
            local compOperator=$( echo "${r"${completionString}"}" | awk '{print $1}' )
            local compOptions=$( echo "${r"${completionString}"}" | awk '{$1="" ; print}' )

            case ${r"${compOperator}"} in
                -A) COMPREPLY=( ${r"$(compgen -A ${compOptions} -- $cur"}) ) ;;
                -W) COMPREPLY=( ${r"$(compgen -W '${compOptions}' -- $cur"}) ) ;;
                 *) COMPREPLY=() ;;
            esac

        fi
        return 0
    fi

    # We must create a list of the valid remaining arguments:

    # Create a list of all arguments that are
    # mutually exclusive with arguments we have already specified
    local mutex_list=""
    for prevArg in ${r"${resolvedArgList[@]}"} ; do
        if [[ "${r"${MUTUALLY_EXCLUSIVE_ARGS[@]}"}" == *"${r"${prevArg}"};"* ]] ; then
            local mutexArgs=$( echo "${r"${MUTUALLY_EXCLUSIVE_ARGS[@]}"}" | sed -e "s#.*${r"${prevArg}"};\([a-zA-Z0-9_,\-]*\) .*#\1#g" -e "s#,# --#g" -e "s#^#--#g" )
            mutex_list="${r"${mutex_list}${mutexArgs}"}"
        fi
    done

    local remaining_legal_arguments=()
    for (( i=0; i < ${r"${#NORMAL_COMPLETION_ARGUMENTS[@]}"} ; i++ )) ; do
        local legalArg=${r"${NORMAL_COMPLETION_ARGUMENTS[i]}"}
        local okToAdd=true

        # Get the number of times this has occurred in the arguments already:
        local numPrevOccurred=$( grep -o -- "${r"${legalArg}"}" <<< "${r"${resolvedArgList[@]}"}" | wc -l | awk '{print $1}' )

        if [[ $numPrevOccurred -lt "${r"${MAX_OCCURRENCES[i]}"}" ]] ; then

            # Make sure this arg isn't mutually exclusive to another argument that we've already had:
            if [[ "${r"${mutex_list}"}" ==    "${r"${legalArg}"} "* ]] ||
               [[ "${r"${mutex_list}"}" ==  *" ${r"${legalArg}"} "* ]] ||
               [[ "${r"${mutex_list}"}" ==  *" ${r"${legalArg}"}"  ]] ; then
                okToAdd=false
            fi

            # Check if we're still good to add in the argument:
            if $okToAdd ; then
                # Add in the argument:
                ${r"remaining_legal_arguments+=($legalArg)"}

                # Add in the synonyms of the argument:
                if [[ "${r"${SYNONYMOUS_ARGS[@]}"}" == *"${r"${legalArg}"}"* ]] ; then
                    local synonymString=$( echo "${r"${SYNONYMOUS_ARGS[@]}"}" | sed -e "s#.*${r"${legalArg}"};\([a-zA-Z0-9_,\-]*\).*#\1#g" -e "s#,# #g"  )
                    ${r"remaining_legal_arguments+=($synonymString)"}
                fi
            fi
        fi

    done

    # Add in the special option "--" which separates tool options from meta-options:
    if [[ $# -eq 0 ]] ; then
        remaining_legal_arguments+=("--")
    fi

    COMPREPLY=( ${r"$(compgen -W '${remaining_legal_arguments[@]}' -- $cur"}) )
    return 0
}

####################################################################################################

_masterCompletionFunction()
{
    # Set up global variables for the functions that do completion:
    prev=${r"${COMP_WORDS[COMP_CWORD-1]}"}
    cur=${r"${COMP_WORDS[COMP_CWORD]}"}

    NUM_POSITIONAL_ARGUMENTS=0
    POSITIONAL_ARGUMENT_TYPE=()

    # The set of all legal arguments
    # Corresponds by index to the type of those arguments in ALL_ARGUMENT_VALUE_TYPES
    ALL_LEGAL_ARGUMENTS=()

    # The types of all legal arguments
    # Corresponds by index to the names of those arguments in ALL_LEGAL_ARGUMENTS
    ALL_ARGUMENT_VALUE_TYPES=()

    # Arguments that are mutually exclusive.
    # These are listed here as arguments concatenated together with delimiters:
    # ${r"<"}Main argument${r">"};${r"<"}Mutex Argument 1${r">"}[,${r"<"}Mutex Argument 2${r">"},...]
    MUTUALLY_EXCLUSIVE_ARGS=()

    # Alternate names of arguments.
    # These are listed here as arguments concatenated together with delimiters.
    # ${r"<"}Main argument${r">"};${r"<"}Synonym Argument 1${r">"}[,${r"<"}Synonym Argument 2${r">"},...]
    SYNONYMOUS_ARGS=()

    # The minimum number of times an argument can occur.
    MIN_OCCURRENCES=()

    # The maximum number of times an argument can occur.
    MAX_OCCURRENCES=()

    # Set up locals for this function:
    local toolName=$( _getToolName )

    # Check if we have we gone onto the post-tool options:
    if [[ "${r"${COMP_WORDS[@]}"}" == *" -- "* ]] ; then
        NUM_POSITIONAL_ARGUMENTS=0
        POSITIONAL_ARGUMENT_TYPE=()
        DEPENDENT_ARGUMENTS=()
        NORMAL_COMPLETION_ARGUMENTS=("${r"${CS_NORMAL_COMPLETION_ARGUMENTS[@]}"}")
        MUTUALLY_EXCLUSIVE_ARGS=("${r"${CS_MUTUALLY_EXCLUSIVE_ARGS[@]}"}")
        SYNONYMOUS_ARGS=("${r"${CS_SYNONYMOUS_ARGS[@]}"}")
        MIN_OCCURRENCES=("${r"${CS_MIN_OCCURRENCES[@]}"}")
        MAX_OCCURRENCES=("${r"${CS_MAX_OCCURRENCES[@]}"}")
        ALL_LEGAL_ARGUMENTS=("${r"${CS_ALL_LEGAL_ARGUMENTS[@]}"}")
        ALL_ARGUMENT_VALUE_TYPES=("${r"${CS_ALL_ARGUMENT_VALUE_TYPES[@]}"}")

        # Complete the arguments for the base script:
        # Strictly speaking, what the argument to this function is doesn't matter.
        _handleArgs CALLER_SCRIPT

<@emitGroupToolCheckConditional tools=tools/>
    else
        <@compress_single_line>
        COMPREPLY=( $(compgen -W "<@emitToolListForTopLevelComplete tools=tools />" -- ${r"$"}cur) )
        </@compress_single_line>

    fi
}

${r"complete -o default -F _masterCompletionFunction ${CALLER_SCRIPT_NAME}"}



