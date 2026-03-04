package dslabs.primarybackup;

import dslabs.framework.Message;
import lombok.Data;

// additional imports by me
import dslabs.framework.Application;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Request implements Message {
  // Your code here...
  private final AMOCommand command;
}

@Data
class Reply implements Message {
  // Your code here...
  private final AMOResult result;
}

// Your code here...

@Data
class ForwardRequest implements Message {
  private final AMOCommand command;
  private final int viewNum;
}

@Data
class ForwardReply implements Message {
  private final AMOCommand command;
  private final AMOResult result;
  private final int viewNum;
}

@Data
class StateTransfer implements Message {
  private final AMOApplication<Application> app;
  private final int viewNum;
}

@Data
class StateTransferReply implements Message {
  private final int viewNum;
}
