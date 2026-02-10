package dslabs.atmostonce;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

// additional imports by me
import java.util.HashMap;
import dslabs.framework.Address;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;

  // Your code here...
  private final HashMap<Address, AMOResult> lastResults = new HashMap<>(); // pretend this is `struct AMOApplication { lastResults: HashMap<Address, AMOResult> }` owned and immutable for which we remember the last answer we gave to each client

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;

    // Your code here...
    if (alreadyExecuted(amoCommand)) return lastResults.get(amoCommand.clientId()); // if we've already executed this command for this client, return the last result
    Result result = application.execute(amoCommand.command()); // if we haven't executed this command for this client, execute it and store the result
    AMOResult amoResult = new AMOResult(result, amoCommand.sequenceNum()); // wrap the result with the sequenceNum so client can match it
    lastResults.put(amoCommand.clientId(), amoResult); // store the result in our lastResults map
    return amoResult; // return the result

  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    // Your code here...
    AMOResult last = lastResults.get(amoCommand.clientId());
    // we already executed if we have a saved result for this client and that saved result's sequenceNum is >= this command's sequenceNum
    return last != null && last.sequenceNum() >= amoCommand.sequenceNum();
  }
}
