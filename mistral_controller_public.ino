#include <Arduino.h>
#include <ESP8266WiFi.h>
#include <ESP8266WebServer.h>
#include <ESP8266HTTPUpdateServer.h>
#include <AsyncMqttClient.h>
#include "config.h"
#include "mqtt_config.h"

static_assert(MISTRAL_PR_NUMBER>=1&&MISTRAL_PR_NUMBER<=4,"MISTRAL_PR_NUMBER must be 1-4");
constexpr uint8_t PR_NUMBER=MISTRAL_PR_NUMBER;
constexpr uint8_t DE=5;
constexpr uint16_t CAP=256;
constexpr uint8_t RAW_BYTES=64,IDLE_CAP=16,ACTIVE_CAP=32,FIRST_CAP=8;
constexpr uint8_t DISCOVERY_COUNT=4;
ESP8266WebServer web(80);
ESP8266HTTPUpdateServer httpUpdater;
enum Stage:uint8_t {IDLE,ACTIVE,DONE};
enum ControlKind:uint8_t {CONTROL_NONE,CONTROL_GEAR,CONTROL_FAN};
enum ControlState:uint8_t {CONTROL_IDLE,CONTROL_WAIT_SEND,CONTROL_WAIT_CONFIRM,CONTROL_TAIL};
Stage stage=IDLE;
AsyncMqttClient mqtt;
ControlKind controlKind=CONTROL_NONE;
ControlState controlState=CONTROL_IDLE;
const char* reason="not_armed";
const char* lastResult="none";
struct Row {uint32_t ms;uint16_t frame,lag,de,wr,wait,tail,pause,pending;uint8_t gear,mask,kind;};
Row rows[CAP];
const char* kinds[]={"sent_sample","skipped_uncertain_rx","skipped_frame_time","skipped_pending_rx","skipped_stale","lost_main_view","tx_fault","tx_not_empty","main_view_seen","main_view_returned","partial_timeout","parser_overflow","controller_fault","http_rx_uncertain","control_key_sent","mqtt_rx_uncertain"};
const char* outcomes[]={"sent","skipped_uncertain_rx","skipped_frame_time","skipped_pending_rx","skipped_stale","lost_main_view","tx_fault","tx_not_empty","deadline","passive_idle","waiting_accepted_report"};
struct Raw {uint32_t uptime,at,seq;uint16_t wireLen,bodyLen;uint8_t gear,mask,outcome;uint8_t wire[RAW_BYTES],body[RAW_BYTES];};
Raw idleRaw[IDLE_CAP],activeRaw[ACTIVE_CAP];
Raw transitionRaw;
bool transitionRawStored=false;
struct RawStats {uint32_t reports=0,total=0,lastAt=0,changes=0,truncated=0;uint16_t signatureLen=0;uint8_t signature[6]={};bool seen=false;};
RawStats idleStats,activeStats;
uint8_t idleStored=0,idleNext=0,activeFixed=0,activeRecent=0,activeNext=FIRST_CAP;
uint8_t reportOutcome=8;
uint16_t stored=0,nextRow=0,contextPause=0,contextPending=0;
uint8_t wire[256],body[256],header=0,mask=0;
uint8_t reply[]={255,255,0x68,0xfe,MISTRAL_PR_NUMBER,0xc4,0,0,0x90,0,0,0x16};
size_t used=0,decodedLen=0;
int emptySpace=0;
bool fault=false,ready=false,seen=false,httpSlot=false,haveReport=false;
uint32_t lastByteAt=0,parserAt=0,frameAt=0,armedAt=0,firstMain=0,elapsed=0,slotAt=0,lastHttpMs=0;
uint32_t frames=0,reports=0,mains=0,others=0,sent=0,sentMain=0,losses=0;
uint32_t decodedReports01=0,decodedReports02=0,decodedOtherReports=0;
uint8_t lastOtherBody[256]={};
uint16_t lastOtherBodyLen=0;
uint32_t lastOtherReportMs=0;
uint8_t currentGear=0,observedGear=0,gearExpected=0,gearAcks=0,gearTailReplies=0,gearKeyTxCount=0,gearReplyCount=0;
uint8_t controlAcks=0,controlTailReplies=0,controlKeyTxCount=0,fanPhase=0,fanSupplyCandidate=255,fanTarget=255;
uint8_t mainSupplyCandidate=255,mainSupplyCount=0;
int8_t fanSupplyState=-1,fanSupplyOrigin=-1;
uint16_t repliesSinceMain=0;
uint32_t fanSupplyObservedAt=0,lastGearAt=0;
uint32_t controlStartedAt=0,controlPhaseAt=0,controlNextAt=0,controlSentAt=0;
const uint8_t fanKeys[]={0x0A,0x06,0x06,0x06,0x01,0x07,0x06,0x09};
const uint8_t view0[]={0x07,0x00,0x00,0x11,0x00,0x0F,0x00,0x00,0x04,0x41,0x40,0x06,0x00,0xD0,0x73,0x00,0x01,0x3A};
const uint8_t view1[]={0x07,0x00,0x00,0x11,0x00,0x0F,0x00,0x00,0x04,0x41,0x40,0x5B,0x00,0xD0,0x73,0x00,0x01,0x3A};
const uint8_t view2[]={0x0F,0x00,0x00,0x11,0x00,0x00,0x00,0x00,0x04,0x41,0x40,0x38,0x76,0xD0,0x73,0x00,0x01,0x3A};
const uint8_t view3[]={0x0F,0x00,0x00,0x11,0x00,0x00,0x00,0x80,0x00,0x45,0x40,0x00,0x00,0x00,0x00,0x00,0x01,0x42};
const uint8_t view4[]={0x0F,0x00,0x00,0x10,0x00,0x10,0x00,0x10,0x00,0x64,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x37};
const uint8_t view5[]={0x0F,0x00,0x00,0x10,0x00,0x10,0x00,0x10,0x00,0x64,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x36};
const uint8_t view6[]={0x0F,0x00,0x00,0x11,0x00,0x00,0x00,0x40,0x00,0x45,0x40,0x00,0x00,0x00,0x00,0x00,0x01,0x42};
uint32_t skipUncertain=0,skipFrame=0,skipPending=0,skipStale=0;
uint32_t partials=0,overflows=0,partialBefore=0,partialAfter=0,overflowTest=0;
uint32_t peak=0,total=0,lastSample=0,lastReportMs=0,maxReportGap=0;
uint32_t yieldRx=0,acceptedStarts=0,uncertainStarts=0,maxYieldRx=0,maxPending=0;
uint32_t httpCalls=0,httpRequests=0,httpStatusRequests=0,maxHttpUs=0,httpWithRx=0,httpOver500=0,httpOver5000=0;
IPAddress brokerIP;
bool mqttConfigOK=false,connecting=false,onlinePublished=false,mqttSubscribed=false,stateDirty=true;
bool discoveryComplete=false,discoveryFailed=false;
uint32_t stateVersion=1,flightStateVersion=0;
volatile bool mqttUp=false;
volatile uint32_t mqttConnects=0,mqttDisconnects=0;
volatile uint16_t ackMailbox=0,commandSubscription=0;
volatile uint8_t lastDisconnect=0;
uint32_t mqttAttempts=0,mqttPublishCalls=0,mqttPublishAcks=0,mqttRejected=0,mqttTimeouts=0;
uint32_t mqttCalls=0,maxMqttUs=0,mqttWithRx=0,lastAttempt=0,connectAt=0,lastPublish=0,flightAt=0,lastMqttService=0,lastSubscribe=0;
uint16_t flight=0;
uint8_t flightKind=0;
uint32_t handledConnects=0,handledDisconnects=0;
char mqttPrefix[64],availabilityTopic[120],stateTopic[120],commandTopic[120],clientId[64];
char discoveryTopic[128],discoveryPayload[768];
char prSlug[32],prName[32],wifiHostName[48];
uint8_t discoveryIndex=0;
uint32_t discoveryErrors=0,lastDiscoveryAttempt=0;
char activeRequestId[33]="",lastRequestId[33]="",resultRequestId[33]="";
char recentRequestIds[8][33]{};
uint8_t recentRequestNext=0;
char incomingPayload[129];
size_t incomingUsed=0,incomingTotal=0;
uint32_t incomingStartedAt=0;
volatile bool incomingInProgress=false,incomingReady=false;
bool incomingRetained=false,incomingOverflow=false,incomingDropping=false;
uint32_t commandMessages=0,commandRejected=0,retainedRejected=0,duplicateRejected=0;
volatile uint32_t commandMailboxDrops=0;
uint8_t targetGear=0,gearStepsTotal=0;
char statePayload[640];
struct ParsedCommand{char id[33];uint8_t kind;uint8_t gear;int8_t fan;};
void markStateDirty(){stateDirty=true;++stateVersion;}
constexpr uint16_t RXCAP=1024;
struct RxByte {uint32_t us;uint8_t b;};
RxByte rxTail[RXCAP];
uint16_t rxNext=0,rxStored=0,snapshotUsed=0;
uint8_t snapshotHeader=0,snapshotWire[256];
bool rxFrozen=false,recovered=false;
const char* freezeReason="not_frozen";
uint32_t freezeAt=0,snapshotSilence=0,snapshotAge=0,recoveryDelay=0;
uint16_t recoveryLen=0;
uint8_t recoveryBody[256];
void freezeRx(const char* why){
  if(rxFrozen)return;rxFrozen=true;freezeReason=why;freezeAt=millis()-armedAt;
  snapshotUsed=used;snapshotHeader=header;memcpy(snapshotWire,wire,used);
  uint32_t now=micros();snapshotSilence=now-lastByteAt;snapshotAge=now-parserAt;
}
uint16_t clip(uint32_t x){return x>65535?65535:uint16_t(x);}
uint16_t crc(const uint8_t* p,size_t n){uint16_t c=65535;while(n--){c^=*p++;for(int i=0;i<8;++i)c=(c>>1)^((c&1)?0xa001:0);}return c;}
void clearContext(){mask=0;contextPause=contextPending=0;}
void resetParser(){used=0;header=0;}
void record(Row r,uint8_t k){r.kind=k;rows[nextRow]=r;nextRow=(nextRow+1)%CAP;if(stored<CAP)++stored;++total;}
void event(uint8_t k){Row r{};r.ms=millis()-armedAt;r.mask=mask;r.pause=contextPause;r.pending=contextPending;record(r,k);}
void stop(const char* why){
  if(controlKind!=CONTROL_NONE)failControl("bus_fault");
  freezeRx(why);digitalWrite(DE,LOW);elapsed=millis()-armedAt;reason=why;stage=DONE;httpSlot=false;event(12);
}
bool knownB5(uint8_t b){return b==0x01||b==0x02||b==0x08||b==0x10||b==0x20;}
uint8_t getGear(){if(body[4]!=0x80||!knownB5(body[5]))return 0;switch(body[3]){case 1:return 1;case 3:return 2;case 5:return 3;case 7:return 4;default:return 0;}}
bool viewMatches(const uint8_t* expected){return decodedLen==23&&memcmp(body+3,expected,18)==0;}
int8_t fanObservation(uint8_t gear){
  switch(fanPhase){
    case 0:return viewMatches(view0)?2:-1;
    case 1:return viewMatches(view1)?2:-1;
    case 2:return viewMatches(view2)?2:-1;
    case 3:if(viewMatches(view3))return 1;if(viewMatches(view6))return 0;return -1;
    case 4:return viewMatches(fanSupplyOrigin?view4:view5)?2:-1;
    case 5:return viewMatches(fanSupplyOrigin?view5:view4)?2:-1;
    case 6:return viewMatches(fanTarget?view3:view6)?2:-1;
    case 7:return gear?2:-1;
    default:return -1;
  }
}
const char* controlKindName(){return controlKind==CONTROL_GEAR?"gear":controlKind==CONTROL_FAN?"fan":"none";}
const char* controlStateName(){return controlState==CONTROL_WAIT_SEND?"waiting_to_send":controlState==CONTROL_WAIT_CONFIRM?"awaiting_confirmation":controlState==CONTROL_TAIL?"neutral_tail":"idle";}
void finishControl(const char* result){
  if(activeRequestId[0]){strncpy(resultRequestId,activeRequestId,sizeof(resultRequestId)-1);resultRequestId[sizeof(resultRequestId)-1]=0;}
  lastResult=result;activeRequestId[0]=0;controlKind=CONTROL_NONE;controlState=CONTROL_IDLE;
  markStateDirty();
}
void failControl(const char* result){
  if(controlKind==CONTROL_FAN&&controlKeyTxCount){fanSupplyState=-1;fanSupplyObservedAt=0;mainSupplyCandidate=255;mainSupplyCount=0;}
  finishControl(result);
}
void checkControlDeadline(){
  uint32_t now=millis();
  if(controlKind==CONTROL_NONE)return;
  if(uint32_t(now-controlStartedAt)>=60000)failControl("operation_timeout");
  else if(controlState==CONTROL_WAIT_SEND&&uint32_t(now-controlPhaseAt)>=5000)failControl("no_safe_send_window");
  else if(controlState==CONTROL_WAIT_CONFIRM&&uint32_t(now-controlSentAt)>=1500)failControl("confirmation_timeout");
  else if(controlState==CONTROL_TAIL&&uint32_t(now-controlPhaseAt)>=2000)failControl("neutral_tail_timeout");
}
void confirmFanPhase(uint8_t observed){
  if(fanPhase==3){fanSupplyState=int8_t(observed);fanSupplyOrigin=fanSupplyState;fanSupplyObservedAt=millis();}
  else if(fanPhase==6){fanSupplyState=int8_t(fanTarget);fanSupplyObservedAt=millis();}
  fanSupplyCandidate=255;
  if(fanPhase==7){controlState=CONTROL_TAIL;controlPhaseAt=millis();controlTailReplies=0;lastResult="target_confirmed_waiting_tail";markStateDirty();}
  else{
    fanPhase=(fanPhase==3&&fanSupplyState==int8_t(fanTarget))?7:uint8_t(fanPhase+1);
    controlState=CONTROL_WAIT_SEND;controlPhaseAt=millis();controlNextAt=millis()+300;controlAcks=0;markStateDirty();
  }
}
void report(uint32_t last,uint32_t span){
  ++reports;if(stage==DONE&&!recovered){recovered=true;recoveryDelay=(millis()-armedAt)-elapsed;recoveryLen=decodedLen;memcpy(recoveryBody,body,decodedLen);}if(stage!=ACTIVE)return;reportOutcome=8;
  Row r{};r.ms=millis()-armedAt;r.frame=clip(span);r.gear=getGear();r.mask=mask;r.pause=contextPause;r.pending=contextPending;
  if(haveReport){uint32_t gap=r.ms-lastReportMs;if(gap>maxReportGap)maxReportGap=gap;}lastReportMs=r.ms;haveReport=true;
  bool fanNavigationActive=controlKind==CONTROL_FAN&&controlState!=CONTROL_IDLE;
  bool transition=false;uint8_t transitionCode=8;
  if(controlKind==CONTROL_NONE&&r.gear&&decodedLen==22&&body[6]==0x22){
    uint8_t observed=(body[12]&0x04)?1:0;
    if(mainSupplyCandidate==observed){if(mainSupplyCount<2)++mainSupplyCount;}
    else{mainSupplyCandidate=observed;mainSupplyCount=1;}
    if(mainSupplyCount>=2){
      if(fanSupplyState!=int8_t(observed))markStateDirty();
      fanSupplyState=int8_t(observed);fanSupplyOrigin=fanSupplyState;
      fanSupplyObservedAt=millis();
    }
  }else if(controlKind==CONTROL_NONE){
    mainSupplyCandidate=255;mainSupplyCount=0;
  }
  if(r.gear){
    currentGear=r.gear;lastGearAt=millis();
    if(observedGear!=r.gear){observedGear=r.gear;gearReplyCount=0;markStateDirty();}
    if(!ready){transition=true;transitionCode=seen?9:8;if(seen)repliesSinceMain=0;}
    if(!seen){seen=true;firstMain=r.ms;repliesSinceMain=0;}
    ++mains;ready=true;
  }
  else {
    currentGear=0;++others;reportOutcome=10;
    if(ready){ready=false;if(!fanNavigationActive){++losses;reportOutcome=5;record(r,5);return;}}
    if(seen&&!fanNavigationActive)return;
  }
  if(controlState==CONTROL_WAIT_CONFIRM){
    if(controlKind==CONTROL_GEAR){
      if(r.gear==gearExpected){if(++gearAcks>=2){
        if(gearExpected==targetGear){controlState=CONTROL_TAIL;controlPhaseAt=millis();controlTailReplies=0;lastResult="target_confirmed_waiting_tail";}
        else{controlState=CONTROL_WAIT_SEND;controlPhaseAt=millis();controlNextAt=millis()+300;gearAcks=0;lastResult="step_confirmed";}
        markStateDirty();
      }}else gearAcks=0;
    }else if(controlKind==CONTROL_FAN){
      int8_t observation=fanObservation(r.gear);
      if(observation<0){controlAcks=0;fanSupplyCandidate=255;}
      else if(fanPhase==3){
        if(fanSupplyCandidate==255||fanSupplyCandidate!=uint8_t(observation)){fanSupplyCandidate=uint8_t(observation);controlAcks=1;}
        else if(++controlAcks>=2)confirmFanPhase(uint8_t(observation));
      }else if(++controlAcks>=2)confirmFanPhase(fanPhase==6?fanTarget:uint8_t(observation));
    }
  }
  // Keep the controller session responsive while the main view is not classified.
  if(transition)record(r,transitionCode);
  if(mask){++skipUncertain;reportOutcome=1;record(r,1);return;}
  if(span<1200||span>4000){++skipFrame;reportOutcome=2;record(r,2);return;}
  if(Serial.available()){++skipPending;reportOutcome=3;record(r,3);return;}
  if(emptySpace<12||Serial.availableForWrite()!=emptySpace){fault=true;reportOutcome=7;record(r,7);stop("tx_not_empty");return;}
  uint32_t t0=micros();r.lag=clip(t0-last);if(uint32_t(t0-last)>500){++skipStale;reportOutcome=4;record(r,4);return;}
  uint8_t txEvent=0;
  if(controlState==CONTROL_WAIT_SEND&&int32_t(millis()-controlNextAt)>=0){
    if(controlKind==CONTROL_GEAR&&r.gear&&gearReplyCount>=9){
      if(observedGear==targetGear){finishControl("already_at_target");}
      else if(gearKeyTxCount>=3){failControl("gear_step_limit");}
      else{
        gearExpected=uint8_t(observedGear+(targetGear>observedGear?1:-1));
        if(gearExpected>=1&&gearExpected<=4)txEvent=targetGear>observedGear?0x08:0x07;
        else failControl("invalid_gear_transition");
      }
    }else if(controlKind==CONTROL_FAN&&fanPhase<8)txEvent=fanKeys[fanPhase];
  }
  reply[6]=txEvent;uint16_t replyCrc=crc(reply+3,6);reply[9]=uint8_t(replyCrc);reply[10]=uint8_t(replyCrc>>8);
  digitalWrite(DE,HIGH);delayMicroseconds(2);uint32_t t1=micros();size_t written=Serial.write(reply,sizeof(reply));uint32_t t2=micros();bool timeout=false;
  while(Serial.availableForWrite()!=emptySpace){if(uint32_t(micros()-t0)>=1500){timeout=true;break;}}
  uint32_t t3=micros();if(!timeout)delayMicroseconds(200);uint32_t t4=micros();digitalWrite(DE,LOW);uint32_t duration=micros()-t0;
  r.wr=clip(t2-t1);r.wait=clip(t3-t2);r.tail=clip(t4-t3);r.de=clip(duration);bool newPeak=duration>peak;if(newPeak)peak=duration;
  if(timeout||written!=sizeof(reply)||duration>1500){fault=true;reportOutcome=6;if(txEvent)failControl("key_tx_fault");record(r,6);stop(timeout?"fifo_wait_timeout":written!=sizeof(reply)?"short_write":"tx_over_1500us");return;}
  ++sent;if(r.gear){++sentMain;if(repliesSinceMain<65535)++repliesSinceMain;if(gearReplyCount<255)++gearReplyCount;}reportOutcome=0;slotAt=micros();httpSlot=true;
  if(txEvent){controlState=CONTROL_WAIT_CONFIRM;controlSentAt=millis();controlAcks=0;gearAcks=0;fanSupplyCandidate=255;++controlKeyTxCount;if(controlKind==CONTROL_GEAR)++gearKeyTxCount;markStateDirty();}
  record(r,txEvent?14:0);lastSample=r.ms;
  if(controlState==CONTROL_TAIL&&r.gear&&txEvent==0&&++controlTailReplies>=2)finishControl("confirmed");
}
void captureRaw(Stage phase,size_t wn,size_t bn,uint32_t uptime,uint32_t at,uint8_t capturedMask,bool viewTransition){
  RawStats& stats=phase==IDLE?idleStats:activeStats;++stats.reports;
  bool changed=!stats.seen||stats.signatureLen!=bn||memcmp(stats.signature,body,6)!=0;
  if(changed){if(stats.seen)++stats.changes;memcpy(stats.signature,body,6);stats.signatureLen=bn;stats.seen=true;}
  bool force=phase==IDLE?stats.total==0:activeFixed<FIRST_CAP;
  if(!force&&!changed&&!viewTransition&&uint32_t(uptime-stats.lastAt)<2000)return;
  Raw* dest;
  if(phase==IDLE){dest=&idleRaw[idleNext];idleNext=(idleNext+1)%IDLE_CAP;if(idleStored<IDLE_CAP)++idleStored;}
  else if(activeFixed<FIRST_CAP)dest=&activeRaw[activeFixed++];
  else {dest=&activeRaw[activeNext];if(++activeNext>=ACTIVE_CAP)activeNext=FIRST_CAP;if(activeRecent<ACTIVE_CAP-FIRST_CAP)++activeRecent;}
  Raw& r=*dest;r.uptime=uptime;r.at=at;r.seq=stats.reports;r.wireLen=wn;r.bodyLen=bn;r.gear=getGear();r.mask=capturedMask;r.outcome=phase==IDLE?9:reportOutcome;
  memcpy(r.wire,wire,wn>RAW_BYTES?RAW_BYTES:wn);memcpy(r.body,body,bn>RAW_BYTES?RAW_BYTES:bn);
  if(phase==ACTIVE&&viewTransition){transitionRaw=r;transitionRawStored=true;}
  if(wn>RAW_BYTES||bn>RAW_BYTES)++stats.truncated;stats.lastAt=uptime;++stats.total;
}
void startFrame(uint32_t t){wire[0]=255;wire[1]=255;wire[2]=0x68;used=3;header=0;parserAt=frameAt=t;}
size_t decode(){if(used<9)return 0;size_t n=0;for(size_t i=3;i<used-1;++i){uint8_t b=wire[i];if(b==0x80){if(++i>=used-1)return 0;b=wire[i]^255;}body[n++]=b;}if(n<5)return 0;return crc(body,n-2)==(uint16_t(body[n-2])|(uint16_t(body[n-1])<<8))?n:0;}
void process(uint8_t b){
  uint32_t now=micros();lastByteAt=now;
  if(stage==ACTIVE&&!rxFrozen){rxTail[rxNext].us=now;rxTail[rxNext].b=b;rxNext=(rxNext+1)%RXCAP;if(rxStored<RXCAP)++rxStored;}
  if(used>=2&&wire[used-2]==255&&wire[used-1]==255&&b==0x68){startFrame(now);return;}
  if(!used){if(!header){if(b==255){header=1;parserAt=now;}}else if(header==1)header=b==255?2:0;else if(b==0x68)startFrame(now);else header=b==255?2:0;return;}
  if(used>=sizeof(wire)){++overflows;if(stage==ACTIVE){++overflowTest;event(11);}resetParser();mask|=2;if(b==255){header=1;parserAt=now;}return;}
  wire[used++]=b;if(b!=0x16)return;size_t n=decode();if(!n)return;
  uint32_t span=now-frameAt;size_t wn=used;resetParser();++frames;
  if(n>=8&&body[0]==1&&body[1]==0xfe&&body[2]==0x44)++decodedReports01;
  else if(n>=8&&body[0]==2&&body[1]==0xfe&&body[2]==0x44)++decodedReports02;
  else{
    ++decodedOtherReports;lastOtherBodyLen=n>sizeof(lastOtherBody)?sizeof(lastOtherBody):n;
    memcpy(lastOtherBody,body,lastOtherBodyLen);lastOtherReportMs=millis();
  }
  if(n>=8&&body[0]==PR_NUMBER&&body[1]==0xfe&&body[2]==0x44){
    Stage phase=stage;uint8_t capturedMask=mask;uint32_t uptime=millis(),at=phase==ACTIVE?uptime-armedAt:0;
    bool wasReady=ready;decodedLen=n;report(now,span);if(phase!=DONE)captureRaw(phase,wn,n,uptime,at,capturedMask,wasReady!=ready);
  }
  clearContext();
}
void receive(){for(unsigned i=0;i<256&&Serial.available();++i){int b=Serial.read();if(b>=0)process(uint8_t(b));}}
void partial(){if((used||header)&&uint32_t(micros()-lastByteAt)>=5000){if(Serial.available()){mask|=2;return;}++partials;if(stage==ACTIVE){if(seen){++partialAfter;freezeRx("first_partial_after_accepted_report");}else ++partialBefore;event(10);}resetParser();mask|=2;}}
const char* stageName(){return stage==IDLE?"idle":stage==ACTIVE?"active":"finished";}
void num(String& s,const char* k,uint32_t v){s+=",\"";s+=k;s+="\":";s+=v;}
void appendHex(String& s,const uint8_t* p,size_t n);
void status(){
  if(stage==ACTIVE){++httpRequests;++httpStatusRequests;}
  String s;s.reserve(4200);s="{\"firmware\":\"mistral-controller-0.31.5\",\"stage\":\"";s+=stageName();s+="\",\"reason\":\"";s+=reason;s+="\",\"fault\":";s+=fault?"true":"false";num(s,"pr_number",PR_NUMBER);
  s+=",\"control_kind\":\"";s+=controlKindName();s+="\",\"control_state\":\"";s+=controlStateName();s+='"';
  s+=",\"active_request_id\":\"";s+=activeRequestId;s+="\",\"last_request_id\":\"";s+=lastRequestId;s+="\",\"last_result\":\"";s+=lastResult;s+='"';
  s+=",\"target_gear\":";if(controlKind==CONTROL_GEAR)s+=unsigned(targetGear);else s+="null";
  s+=",\"target_fan\":";if(controlKind==CONTROL_FAN)s+=fanTarget?"\"ON\"":"\"OFF\"";else s+="null";
  num(s,"expected_gear",gearExpected);num(s,"gear_key_tx_count",gearKeyTxCount);num(s,"gear_confirmation_reports",gearAcks);num(s,"gear_neutral_tail_replies",gearTailReplies);num(s,"control_key_tx_count",controlKeyTxCount);num(s,"control_confirmation_reports",controlAcks);num(s,"control_neutral_tail_replies",controlTailReplies);num(s,"fan_phase",fanPhase);
  s+=",\"fan_supply_initial_state\":";if(fanSupplyOrigin<0)s+="null";else s+=int(fanSupplyOrigin);
  s+=",\"fan_supply_state\":";if(fanSupplyState<0)s+="null";else s+=int(fanSupplyState);
  num(s,"current_gear",currentGear);num(s,"observed_gear",observedGear);num(s,"gear_replies_since_change",gearReplyCount);num(s,"neutral_replies_since_main",repliesSinceMain);
  num(s,"mqtt_enabled",MQTT_ENABLED);num(s,"mqtt_config_ok",mqttConfigOK);num(s,"mqtt_connected",mqttUp);num(s,"mqtt_subscribed",mqttSubscribed);num(s,"mqtt_discovery_complete",discoveryComplete);num(s,"mqtt_discovery_failed",discoveryFailed);num(s,"mqtt_discovery_index",discoveryIndex);num(s,"mqtt_discovery_errors",discoveryErrors);num(s,"mqtt_connect_attempts",mqttAttempts);num(s,"mqtt_connects",mqttConnects);num(s,"mqtt_disconnects",mqttDisconnects);num(s,"mqtt_last_disconnect_reason",lastDisconnect);num(s,"mqtt_publish_calls",mqttPublishCalls);num(s,"mqtt_publish_acks",mqttPublishAcks);num(s,"mqtt_publish_rejected",mqttRejected);num(s,"mqtt_timeouts",mqttTimeouts);num(s,"mqtt_inflight_id",flight);num(s,"mqtt_commands",commandMessages);num(s,"mqtt_command_rejected",commandRejected);num(s,"mqtt_retained_rejected",retainedRejected);num(s,"mqtt_duplicate_rejected",duplicateRejected);num(s,"mqtt_command_mailbox_drops",commandMailboxDrops);num(s,"mqtt_service_calls",mqttCalls);num(s,"max_mqtt_service_us",maxMqttUs);num(s,"mqtt_service_with_rx",mqttWithRx);
  num(s,"valid_crc_frames",frames);num(s,"reports_for_pr",reports);num(s,"main_reports_for_pr",mains);num(s,"nonmain_reports_for_pr",others);num(s,"sent",sent);num(s,"sent_main",sentMain);num(s,"sent_nonmain",sent-sentMain);num(s,"main_view_losses",losses);
  num(s,"decoded_reports_01",decodedReports01);num(s,"decoded_reports_02",decodedReports02);num(s,"decoded_other_reports",decodedOtherReports);s+=",\"last_other_report_body\":\"";appendHex(s,lastOtherBody,lastOtherBodyLen);s+="\"";
  num(s,"skipped_uncertain_rx",skipUncertain);num(s,"skipped_frame_time",skipFrame);num(s,"skipped_pending_rx",skipPending);num(s,"skipped_stale",skipStale);num(s,"partial_timeouts",partials);num(s,"partial_before_first_main",partialBefore);num(s,"partial_after_first_main",partialAfter);num(s,"parser_overflows",overflows);num(s,"parser_overflows_during_test",overflowTest);
  num(s,"rx_capture_stored",rxStored);num(s,"rx_capture_frozen",rxFrozen);num(s,"rx_freeze_at_ms",freezeAt);
  num(s,"parser_snapshot_used",snapshotUsed);num(s,"parser_snapshot_header",snapshotHeader);
  num(s,"first_report_after_stop_seen",recovered);num(s,"first_report_after_stop_delay_ms",recoveryDelay);
  num(s,"max_de_high_us",peak);num(s,"elapsed_ms",stage==ACTIVE?millis()-armedAt:elapsed);num(s,"trace_total_events",total);num(s,"trace_stored",stored);num(s,"trace_overwritten",total-stored);num(s,"empty_fifo_free_space",emptySpace);
  num(s,"yield_with_rx",yieldRx);num(s,"accepted_rx_starts",acceptedStarts);num(s,"uncertain_rx_starts",uncertainStarts);num(s,"max_yield_with_rx_us",maxYieldRx);num(s,"max_pending_after_yield",maxPending);num(s,"max_report_gap_ms",maxReportGap);num(s,"last_report_at_ms",lastReportMs);
  num(s,"http_service_calls",httpCalls);num(s,"http_requests_during_test",httpRequests);num(s,"http_status_requests_during_test",httpStatusRequests);num(s,"max_http_service_us",maxHttpUs);num(s,"http_service_with_rx",httpWithRx);num(s,"http_service_over_500us",httpOver500);num(s,"http_service_over_5000us",httpOver5000);
  num(s,"raw_idle_reports_seen",idleStats.reports);num(s,"raw_idle_samples_total",idleStats.total);num(s,"raw_idle_stored",idleStored);num(s,"raw_idle_overwritten",idleStats.total-idleStored);num(s,"raw_idle_classifier_changes",idleStats.changes);
  num(s,"raw_active_reports_seen",activeStats.reports);num(s,"raw_active_samples_total",activeStats.total);num(s,"raw_active_stored",activeFixed+activeRecent);num(s,"raw_active_overwritten",activeStats.total-activeFixed-activeRecent);num(s,"raw_active_classifier_changes",activeStats.changes);num(s,"raw_truncated_samples",idleStats.truncated+activeStats.truncated);num(s,"free_heap",ESP.getFreeHeap());
  s+=",\"first_main_report_ms\":";if(seen)s+=firstMain;else s+="null";num(s,"post_main_elapsed_ms",seen?(stage==ACTIVE?millis()-armedAt:elapsed)-firstMain:0);s+=",\"last_view_main\":";s+=ready?"true":"false";s+='}';web.send(200,"application/json",s);
}
void tracePage(){
  if(stage==ACTIVE)++httpRequests;if(stage!=DONE){web.send(409,"text/plain","Trace after test only.");return;}
  web.setContentLength(CONTENT_LENGTH_UNKNOWN);web.send(200,"text/csv","");web.sendContent("at_ms,gear,decision,frame_us,parse_to_tx_us,de_high_us,write_us,fifo_wait_us,tail_us,uncertain_mask,pause_us,pending_after_pause\n");
  String batch;batch.reserve(1024);char line[190];uint16_t start=stored==CAP?nextRow:0;
  for(uint16_t i=0;i<stored;++i){const Row& r=rows[(start+i)%CAP];snprintf(line,sizeof(line),"%lu,%u,%s,%u,%u,%u,%u,%u,%u,%u,%u,%u\n",(unsigned long)r.ms,unsigned(r.gear),kinds[r.kind],unsigned(r.frame),unsigned(r.lag),unsigned(r.de),unsigned(r.wr),unsigned(r.wait),unsigned(r.tail),unsigned(r.mask),unsigned(r.pause),unsigned(r.pending));batch+=line;if(batch.length()>=800){web.sendContent(batch);batch="";yield();}}
  if(batch.length())web.sendContent(batch);web.sendContent("");
}
void appendHex(String& s,const uint8_t* p,size_t n){const char* h="0123456789ABCDEF";for(size_t i=0;i<n;++i){if(i)s+=' ';s+=h[p[i]>>4];s+=h[p[i]&15];}}
void mqttService();
void sendRaw(const Raw& r,bool idle){
  String s;s.reserve(680);char head[250];uint8_t b3=r.body[3],b4=r.body[4],b5=r.body[5];
  s+=idle?"idle,":"active,";s+=r.uptime;s+=',';if(!idle)s+=r.at;s+=',';
  snprintf(head,sizeof(head),"%lu,%u,%s,%u,%u,%u,%u,%02X,%02X,%02X,%u,%u,%u,",(unsigned long)r.seq,unsigned(r.gear),outcomes[r.outcome],unsigned(r.mask),unsigned(r.wireLen),unsigned(r.bodyLen),unsigned(r.wireLen>RAW_BYTES||r.bodyLen>RAW_BYTES),unsigned(b3),unsigned(b4),unsigned(b5),unsigned(b4==0x80),unsigned(knownB5(b5)),unsigned(b3==1||b3==3||b3==5||b3==7));
  s+=head;appendHex(s,r.wire,r.wireLen>RAW_BYTES?RAW_BYTES:r.wireLen);s+=',';appendHex(s,r.body,r.bodyLen>RAW_BYTES?RAW_BYTES:r.bodyLen);s+='\n';web.sendContent(s);yield();
}
void rawPage(){
  if(stage==ACTIVE){
    ++httpRequests;
    const Raw* sample=nullptr;
    if(web.hasArg("transition")){
      if(transitionRawStored)sample=&transitionRaw;
      else{web.send(404,"text/plain","No main-view transition captured yet.");return;}
    }else{
      uint16_t count=activeFixed+activeRecent;
      uint16_t index=count?count-1:0;
      if(web.hasArg("index")){
        String value=web.arg("index");
        if(!value.length()||value.length()>2){web.send(400,"text/plain","Invalid sample index.");return;}
        index=0;
        for(size_t i=0;i<value.length();++i){
          if(value[i]<'0'||value[i]>'9'){web.send(400,"text/plain","Invalid sample index.");return;}
          index=index*10+uint8_t(value[i]-'0');
        }
      }
      if(index>=count){web.send(404,"text/plain","Sample index not available.");return;}
      if(index<activeFixed)sample=&activeRaw[index];
      else{
        uint8_t recentStart=activeRecent==ACTIVE_CAP-FIRST_CAP?activeNext:FIRST_CAP;
        uint8_t offset=uint8_t(index-activeFixed);
        uint8_t slot=FIRST_CAP+uint8_t((recentStart-FIRST_CAP+offset)%(ACTIVE_CAP-FIRST_CAP));
        sample=&activeRaw[slot];
      }
    }
    web.setContentLength(CONTENT_LENGTH_UNKNOWN);web.send(200,"text/csv","");web.sendContent("phase,uptime_ms,test_ms,report_seq,gear,decision,uncertain_mask,wire_len,body_len,truncated,b3_hex,b4_hex,b5_hex,gate_b4_80,gate_b5_known,gate_b3_known,wire_hex,body_hex\n");sendRaw(*sample,false);web.sendContent("");return;
  }
  if(stage!=DONE){web.send(409,"text/plain","Raw samples after test only.");return;}
  web.setContentLength(CONTENT_LENGTH_UNKNOWN);web.send(200,"text/csv","");web.sendContent("phase,uptime_ms,test_ms,report_seq,gear,decision,uncertain_mask,wire_len,body_len,truncated,b3_hex,b4_hex,b5_hex,gate_b4_80,gate_b5_known,gate_b3_known,wire_hex,body_hex\n");
  uint8_t start=idleStored==IDLE_CAP?idleNext:0;for(uint8_t i=0;i<idleStored;++i)sendRaw(idleRaw[(start+i)%IDLE_CAP],true);
  for(uint8_t i=0;i<activeFixed;++i)sendRaw(activeRaw[i],false);
  uint8_t recentStart=activeRecent==ACTIVE_CAP-FIRST_CAP?activeNext:FIRST_CAP;
  for(uint8_t i=0;i<activeRecent;++i)sendRaw(activeRaw[FIRST_CAP+(recentStart-FIRST_CAP+i)%(ACTIVE_CAP-FIRST_CAP)],false);
  web.sendContent("");
}
void rxPage(){
  if(stage!=DONE){web.send(409,"text/plain","After test only");return;}
  web.setContentLength(CONTENT_LENGTH_UNKNOWN);web.send(200,"text/csv","");web.sendContent("index,micros_u32,delta_us,byte_hex\n");
  String batch;batch.reserve(1024);char line[80];uint16_t start=rxStored==RXCAP?rxNext:0;uint32_t previous=0;
  for(uint16_t i=0;i<rxStored;++i){const RxByte& r=rxTail[(start+i)%RXCAP];snprintf(line,sizeof(line),"%u,%lu,%lu,%02X\n",unsigned(i),(unsigned long)r.us,(unsigned long)(i?r.us-previous:0),unsigned(r.b));previous=r.us;batch+=line;if(batch.length()>=800){web.sendContent(batch);batch="";yield();}}
  if(batch.length())web.sendContent(batch);web.sendContent("");
}
void parserPage(){
  if(stage!=DONE){web.send(409,"text/plain","After test only");return;}
  String t;t.reserve(1900);t="freeze_reason=";t+=freezeReason;t+="\nfreeze_test_ms=";t+=freezeAt;t+="\nused=";t+=snapshotUsed;t+="\nheader=";t+=unsigned(snapshotHeader);t+="\nsilence_us=";t+=snapshotSilence;t+="\nparser_age_us=";t+=snapshotAge;t+="\nwire_hex=";appendHex(t,snapshotWire,snapshotUsed);
  t+="\nfirst_report_after_stop_seen=";t+=recovered?"1":"0";t+="\nfirst_report_after_stop_delay_ms=";t+=recoveryDelay;t+="\nfirst_report_body_hex=";if(recovered)appendHex(t,recoveryBody,recoveryLen);t+='\n';web.send(200,"text/plain",t);
}
void serviceHttp(){
  if(controlKind!=CONTROL_NONE||!httpSlot)return;if(uint32_t(micros()-slotAt)>2000){httpSlot=false;return;}if(used||header||Serial.available()||uint32_t(millis()-lastHttpMs)<100)return;
  httpSlot=false;lastHttpMs=millis();mqttService();if(used||header||Serial.available()||uint32_t(micros()-slotAt)>2000)return;
  uint32_t t0=micros();web.handleClient();uint32_t dt=micros()-t0;++httpCalls;if(dt>maxHttpUs)maxHttpUs=dt;if(dt>500)++httpOver500;if(dt>5000)++httpOver5000;
  int n=Serial.available();if(n){++httpWithRx;mask|=8;contextPause=clip(dt);contextPending=clip(n);event(13);}
}
void measuredYield(){uint32_t t0=micros();yield();uint32_t dt=micros()-t0;int n=Serial.available();if(n){++yieldRx;contextPause=clip(dt);contextPending=clip(n);if(dt>maxYieldRx)maxYieldRx=dt;if(uint32_t(n)>maxPending)maxPending=n;if(dt>500||n>2){mask|=4;++uncertainStarts;}else ++acceptedStarts;}}
void skipJsonSpace(const char*& p,const char* end){while(p<end&&(*p==' '||*p=='\t'||*p=='\r'||*p=='\n'))++p;}
bool parseJsonString(const char*& p,const char* end,char* out,size_t cap){
  skipJsonSpace(p,end);if(p>=end||*p++!='"')return false;size_t n=0;
  while(p<end&&*p!='"'){char c=*p++;if(c=='\\'||uint8_t(c)<0x20||n+1>=cap)return false;out[n++]=c;}
  if(p>=end||*p++!='"')return false;out[n]=0;return true;
}
bool parseCommandPayload(const char* data,size_t len,ParsedCommand& out){
  const char* p=data;const char* end=data+len;char key[24],value[40];
  bool haveId=false,haveGear=false,haveFan=false;skipJsonSpace(p,end);
  if(p>=end||*p++!='{')return false;
  for(;;){
    if(!parseJsonString(p,end,key,sizeof(key)))return false;skipJsonSpace(p,end);
    if(p>=end||*p++!=':')return false;
    if(!strcmp(key,"request_id")){
      if(haveId||!parseJsonString(p,end,out.id,sizeof(out.id)))return false;haveId=true;
    }else if(!strcmp(key,"gear")){
      if(haveGear||haveFan)return false;skipJsonSpace(p,end);
      if(p>=end||*p<'1'||*p>'4')return false;out.gear=uint8_t(*p++-'0');
      if(p<end&&(*p=='.'||(*p>='0'&&*p<='9')))return false;haveGear=true;
    }else if(!strcmp(key,"fan")){
      if(haveFan||haveGear||!parseJsonString(p,end,value,sizeof(value)))return false;
      if(!strcmp(value,"ON"))out.fan=1;else if(!strcmp(value,"OFF"))out.fan=0;else return false;haveFan=true;
    }else return false;
    skipJsonSpace(p,end);if(p>=end)return false;
    if(*p=='}'){++p;break;}if(*p++!=',')return false;
  }
  skipJsonSpace(p,end);if(p!=end||!haveId||(haveGear==haveFan))return false;
  if(!out.id[0])return false;
  for(const char* q=out.id;*q;++q)if(!((*q>='A'&&*q<='Z')||(*q>='a'&&*q<='z')||(*q>='0'&&*q<='9')||*q=='_'||*q=='-'||*q=='.'))return false;
  out.kind=haveGear?CONTROL_GEAR:CONTROL_FAN;return true;
}
bool requestSeen(const char* id){for(uint8_t i=0;i<8;++i)if(recentRequestIds[i][0]&&!strcmp(recentRequestIds[i],id))return true;return false;}
void rememberRequest(const char* id){
  strncpy(recentRequestIds[recentRequestNext],id,sizeof(recentRequestIds[0])-1);recentRequestIds[recentRequestNext][sizeof(recentRequestIds[0])-1]=0;
  recentRequestNext=uint8_t((recentRequestNext+1)%8);strncpy(lastRequestId,id,sizeof(lastRequestId)-1);lastRequestId[sizeof(lastRequestId)-1]=0;
}
void rejectCommand(const char* result,const char* id=""){
  ++commandRejected;if(id&&id[0]){strncpy(resultRequestId,id,sizeof(resultRequestId)-1);resultRequestId[sizeof(resultRequestId)-1]=0;}else resultRequestId[0]=0;
  lastResult=result;markStateDirty();
}
bool freshMain(){
  return stage==ACTIVE&&ready&&observedGear>=1&&observedGear<=4&&lastGearAt&&uint32_t(millis()-lastGearAt)<=400&&haveReport&&uint32_t(millis()-armedAt-lastReportMs)<=400;
}
void acceptCommand(const ParsedCommand& c){
  if(requestSeen(c.id)){++duplicateRejected;rejectCommand("duplicate_request_id",c.id);return;}
  if(controlKind!=CONTROL_NONE){rejectCommand("busy",c.id);return;}
  if(!freshMain()){rejectCommand("no_fresh_main_report",c.id);return;}
  if(gearReplyCount<9||repliesSinceMain<9){rejectCommand("fewer_than_nine_session_replies",c.id);return;}
  if(c.kind==CONTROL_GEAR){
    if(observedGear==c.gear){rememberRequest(c.id);strncpy(resultRequestId,c.id,sizeof(resultRequestId)-1);resultRequestId[sizeof(resultRequestId)-1]=0;lastResult="already_at_target";markStateDirty();return;}
    rememberRequest(c.id);strncpy(activeRequestId,c.id,sizeof(activeRequestId)-1);activeRequestId[sizeof(activeRequestId)-1]=0;
    targetGear=c.gear;gearStepsTotal=uint8_t(abs(int(targetGear)-int(observedGear)));gearKeyTxCount=gearAcks=gearTailReplies=0;controlKind=CONTROL_GEAR;controlState=CONTROL_WAIT_SEND;
  }else{
    if(observedGear!=1){rejectCommand("fan_control_requires_gear_1",c.id);return;}
    if(fanSupplyState<0||!fanSupplyObservedAt||uint32_t(millis()-fanSupplyObservedAt)>400){rejectCommand("no_fresh_passive_supply_state",c.id);return;}
    if(fanSupplyState==c.fan){rememberRequest(c.id);strncpy(resultRequestId,c.id,sizeof(resultRequestId)-1);resultRequestId[sizeof(resultRequestId)-1]=0;lastResult="already_at_target";markStateDirty();return;}
    rememberRequest(c.id);strncpy(activeRequestId,c.id,sizeof(activeRequestId)-1);activeRequestId[sizeof(activeRequestId)-1]=0;
    fanTarget=uint8_t(c.fan);fanPhase=controlAcks=controlTailReplies=0;controlKeyTxCount=0;fanSupplyOrigin=fanSupplyState;fanSupplyCandidate=255;
    controlKind=CONTROL_FAN;controlState=CONTROL_WAIT_SEND;
  }
  controlStartedAt=controlPhaseAt=controlNextAt=millis();controlKeyTxCount=0;lastResult="accepted";markStateDirty();
}
void processIncomingCommand(){
  if(!incomingReady)return;
  incomingInProgress=true;incomingReady=false;
  if(incomingRetained){++retainedRejected;rejectCommand("retained_command_rejected");incomingInProgress=false;return;}
  if(incomingOverflow){rejectCommand("command_too_large");incomingInProgress=false;return;}
  ++commandMessages;ParsedCommand c{};
  if(!parseCommandPayload(incomingPayload,incomingUsed,c))rejectCommand("invalid_payload");
  else acceptCommand(c);
  incomingInProgress=false;
}
void onMqttMessage(char* topic,char* payload,AsyncMqttClientMessageProperties properties,size_t len,size_t index,size_t total){
  if(strcmp(topic,commandTopic))return;
  if(index==0){
    if(incomingInProgress&&!incomingReady&&uint32_t(millis()-incomingStartedAt)>2000){incomingInProgress=false;incomingUsed=0;incomingDropping=false;++commandMailboxDrops;}
    if(incomingReady||incomingInProgress){++commandMailboxDrops;incomingDropping=true;incomingTotal=total;}
    else{incomingUsed=0;incomingTotal=total;incomingStartedAt=millis();incomingRetained=properties.retain;incomingOverflow=total>=sizeof(incomingPayload);incomingInProgress=true;incomingDropping=false;}
  }
  if(incomingDropping){if(index+len>=total)incomingDropping=false;return;}
  if(!incomingInProgress||total!=incomingTotal||index!=incomingUsed){incomingOverflow=true;}
  if(!incomingOverflow&&index==incomingUsed&&len<=sizeof(incomingPayload)-1-incomingUsed){
    memcpy(incomingPayload+incomingUsed,payload,len);incomingUsed+=len;
  }else if(len>sizeof(incomingPayload)-1-incomingUsed)incomingOverflow=true;
  if(index+len>=total){
    if(!incomingOverflow)incomingPayload[incomingUsed]=0;
    incomingInProgress=false;incomingReady=true;
  }
}
bool centralFresh(){
  return haveReport&&uint32_t(millis()-armedAt-lastReportMs)<=400;
}
void buildStatePayload(){
  uint32_t now=millis();bool gearFresh=ready&&observedGear&&lastGearAt&&uint32_t(now-lastGearAt)<=400;
  bool fanFresh=fanSupplyState>=0&&fanSupplyObservedAt&&uint32_t(now-fanSupplyObservedAt)<=1000;
  const char* gearText=gearFresh?(observedGear==1?"1":observedGear==2?"2":observedGear==3?"3":"4"):"null";
  const char* fanText=fanFresh?(fanSupplyState?"\"ON\"":"\"OFF\""):"null";
  const char* id=activeRequestId[0]?activeRequestId:resultRequestId;
  int target=controlKind==CONTROL_GEAR?targetGear:-1;
  const char* fanTargetText=controlKind==CONTROL_FAN?(fanTarget?"\"ON\"":"\"OFF\""):"null";
  uint8_t step=controlKind==CONTROL_GEAR?gearKeyTxCount:controlKind==CONTROL_FAN?uint8_t(fanPhase+1):0;
  uint8_t totalSteps=controlKind==CONTROL_GEAR?gearStepsTotal:controlKind==CONTROL_FAN?8:0;
  snprintf(statePayload,sizeof(statePayload),
    "{\"firmware\":\"mistral-controller-0.31.5\",\"pr_number\":%u,\"uptime_ms\":%lu,\"central_connected\":%s,\"mqtt_connected\":%s,\"gear\":%s,\"fan_supply\":%s,\"control_kind\":\"%s\",\"control_state\":\"%s\",\"request_id\":\"%s\",\"target_gear\":%s,\"target_fan\":%s,\"step\":%u,\"steps_total\":%u,\"keys_sent\":%u,\"confirmations\":%u,\"last_request_id\":\"%s\",\"last_result\":\"%s\",\"fault\":%s,\"free_heap\":%lu}",
    unsigned(PR_NUMBER),(unsigned long)now,centralFresh()?"true":"false",mqttUp?"true":"false",gearText,fanText,controlKindName(),controlStateName(),id,
    target>0?(target==1?"1":target==2?"2":target==3?"3":"4"):"null",fanTargetText,unsigned(step),unsigned(totalSteps),unsigned(controlKeyTxCount),unsigned(controlKind==CONTROL_GEAR?gearAcks:controlAcks),
    lastRequestId,lastResult,fault?"true":"false",(unsigned long)ESP.getFreeHeap());
}
bool buildDiscoveryPayload(uint8_t index){
  static const char* components[]={"sensor","binary_sensor","select","switch"};
  static const char* objects[]={"gear","central_connected","gear_target","fan"};
  if(index>=DISCOVERY_COUNT)return false;
  int topicLen=snprintf(discoveryTopic,sizeof(discoveryTopic),"homeassistant/%s/%s/%s/config",components[index],prSlug,objects[index]);
  if(topicLen<=0||topicLen>=int(sizeof(discoveryTopic)))return false;
  int n=0;
  switch(index){
    case 0:
      n=snprintf(discoveryPayload,sizeof(discoveryPayload),
        R"json({"name":"%s Gear","unique_id":"%s_gear","state_topic":"%s","value_template":"{{ value_json.gear }}","availability_topic":"%s","payload_available":"online","payload_not_available":"offline","qos":1,"icon":"mdi:fan","device":{"identifiers":["%s_controller"],"name":"%s","model":"ESP8266 controller","sw_version":"0.31.5"}})json",
        prName,prSlug,stateTopic,availabilityTopic,prSlug,prName);
      break;
    case 1:
      n=snprintf(discoveryPayload,sizeof(discoveryPayload),
        R"json({"name":"%s Central connection","unique_id":"%s_central_connected","state_topic":"%s","value_template":"{{ 'ON' if value_json.central_connected else 'OFF' }}","payload_on":"ON","payload_off":"OFF","device_class":"connectivity","availability_topic":"%s","payload_available":"online","payload_not_available":"offline","qos":1,"device":{"identifiers":["%s_controller"],"name":"%s","model":"ESP8266 controller","sw_version":"0.31.5"}})json",
        prName,prSlug,stateTopic,availabilityTopic,prSlug,prName);
      break;
    case 2:
      n=snprintf(discoveryPayload,sizeof(discoveryPayload),
        R"json({"name":"%s Target gear","unique_id":"%s_gear_target","command_topic":"%s","command_template":"{\"request_id\":\"ha-{{ now().strftime('%%Y%%m%%d%%H%%M%%S%%f') }}\",\"gear\":{{ value }}}","state_topic":"%s","value_template":"{{ value_json.gear }}","options":["1","2","3","4"],"availability_topic":"%s","payload_available":"online","payload_not_available":"offline","qos":1,"retain":false,"optimistic":false,"device":{"identifiers":["%s_controller"],"name":"%s","model":"ESP8266 controller","sw_version":"0.31.5"}})json",
        prName,prSlug,commandTopic,stateTopic,availabilityTopic,prSlug,prName);
      break;
    default:
      n=snprintf(discoveryPayload,sizeof(discoveryPayload),
        R"json({"name":"%s Fan supply","unique_id":"%s_fan","command_topic":"%s","command_template":"{\"request_id\":\"ha-{{ now().strftime('%%Y%%m%%d%%H%%M%%S%%f') }}\",\"fan\":\"{{ value }}\"}","payload_on":"ON","payload_off":"OFF","state_topic":"%s","value_template":"{{ value_json.fan_supply }}","availability_topic":"%s","payload_available":"online","payload_not_available":"offline","qos":1,"retain":false,"optimistic":false,"icon":"mdi:fan","device":{"identifiers":["%s_controller"],"name":"%s","model":"ESP8266 controller","sw_version":"0.31.5"}})json",
        prName,prSlug,commandTopic,stateTopic,availabilityTopic,prSlug,prName);
      break;
  }
  return n>0&&n<int(sizeof(discoveryPayload));
}
void mqttWork(){
  if(!MQTT_ENABLED||!mqttConfigOK)return;uint32_t now=millis();
  if(handledDisconnects!=mqttDisconnects){handledDisconnects=mqttDisconnects;connecting=false;flight=0;onlinePublished=false;mqttSubscribed=false;discoveryComplete=false;lastAttempt=now;markStateDirty();}
  if(handledConnects!=mqttConnects){handledConnects=mqttConnects;connecting=false;flight=0;onlinePublished=false;mqttSubscribed=false;discoveryIndex=0;discoveryComplete=false;discoveryFailed=false;lastDiscoveryAttempt=now-250;markStateDirty();}
  uint16_t ack=ackMailbox;
  if(flight&&ack==flight){ackMailbox=0;uint8_t kind=flightKind;flight=0;++mqttPublishAcks;if(kind==1)onlinePublished=true;else if(kind==2&&flightStateVersion==stateVersion)stateDirty=false;else if(kind==3){if(discoveryIndex<DISCOVERY_COUNT)++discoveryIndex;if(discoveryIndex>=DISCOVERY_COUNT)discoveryComplete=true;}}
  if(WiFi.status()!=WL_CONNECTED)return;
  if(!mqttUp){
    if(connecting){if(uint32_t(now-connectAt)>=10000){++mqttTimeouts;connecting=false;lastAttempt=now;mqtt.disconnect(true);}return;}
    if(mqttAttempts&&uint32_t(now-lastAttempt)<15000)return;
    ++mqttAttempts;connecting=true;connectAt=lastAttempt=now;mqtt.connect();return;
  }
  if(!mqttSubscribed&&uint32_t(now-lastSubscribe)>=5000){commandSubscription=mqtt.subscribe(commandTopic,1);lastSubscribe=now;}
  if(flight){if(uint32_t(now-flightAt)>=10000){++mqttTimeouts;flight=0;onlinePublished=false;lastAttempt=now;mqtt.disconnect(true);}return;}
  if(!onlinePublished){
    ackMailbox=0;uint16_t id=mqtt.publish(availabilityTopic,1,true,"online");++mqttPublishCalls;
    if(id){flight=id;flightKind=1;flightAt=now;}else ++mqttRejected;return;
  }
  if(!discoveryComplete&&!discoveryFailed){
    if(uint32_t(now-lastDiscoveryAttempt)<250)return;
    lastDiscoveryAttempt=now;
    if(!buildDiscoveryPayload(discoveryIndex)){++discoveryErrors;discoveryFailed=true;}
    else{
      ackMailbox=0;uint16_t id=mqtt.publish(discoveryTopic,1,true,discoveryPayload);++mqttPublishCalls;
      if(id){flight=id;flightKind=3;flightAt=now;}else ++mqttRejected;
      return;
    }
  }
  if(!mqttSubscribed)return;
  if(!stateDirty&&uint32_t(now-lastPublish)<5000)return;
  lastPublish=now;buildStatePayload();flightStateVersion=stateVersion;ackMailbox=0;uint16_t id=mqtt.publish(stateTopic,1,false,statePayload);++mqttPublishCalls;
  if(id){flight=id;flightKind=2;flightAt=now;}else ++mqttRejected;
}
void mqttService(){
  uint32_t t0=micros();mqttWork();uint32_t dt=micros()-t0;lastMqttService=millis();
  if(stage==ACTIVE){++mqttCalls;if(dt>maxMqttUs)maxMqttUs=dt;int n=Serial.available();if(n){++mqttWithRx;mask|=16;contextPause=clip(dt);contextPending=clip(n);event(15);}}
}
void serviceBackground(){
  if(Serial.available()||used||header||uint32_t(micros()-lastByteAt)<2000)return;
  uint32_t now=millis();if(uint32_t(now-lastMqttService)>=20)mqttService();
  if(controlKind==CONTROL_NONE&&!Serial.available()&&!used&&!header&&uint32_t(now-lastHttpMs)>=100){
    lastHttpMs=now;uint32_t t0=micros();web.handleClient();uint32_t dt=micros()-t0;++httpCalls;
    if(dt>maxHttpUs)maxHttpUs=dt;if(dt>500)++httpOver500;if(dt>5000)++httpOver5000;
    int n=Serial.available();if(n){++httpWithRx;mask|=8;contextPause=clip(dt);contextPending=clip(n);event(13);}
  }
}
void setup(){
  digitalWrite(DE,LOW);pinMode(DE,OUTPUT);Serial.setRxBufferSize(2048);Serial.begin(115200,SERIAL_8N1);Serial.setDebugOutput(false);Serial.swap();emptySpace=Serial.availableForWrite();uint16_t c=crc(reply+3,6);reply[9]=uint8_t(c);reply[10]=uint8_t(c>>8);
  snprintf(prSlug,sizeof(prSlug),"mistral_pr%u",unsigned(PR_NUMBER));
  snprintf(prName,sizeof(prName),"Mistral PR%u",unsigned(PR_NUMBER));
  snprintf(wifiHostName,sizeof(wifiHostName),"mistral-pr%u-controller",unsigned(PR_NUMBER));
  WiFi.persistent(false);WiFi.mode(WIFI_STA);WiFi.hostname(wifiHostName);WiFi.setAutoReconnect(true);WiFi.begin(WIFI_SSID,WIFI_PASSWORD);
  int p=snprintf(mqttPrefix,sizeof(mqttPrefix),"%s/pr%u",MQTT_BASE_TOPIC,unsigned(PR_NUMBER));
  int a=snprintf(availabilityTopic,sizeof(availabilityTopic),"%s/availability",mqttPrefix);
  int b=snprintf(stateTopic,sizeof(stateTopic),"%s/state",mqttPrefix);
  int d=snprintf(commandTopic,sizeof(commandTopic),"%s/cmd",mqttPrefix);
  mqttConfigOK=MQTT_ENABLED&&brokerIP.fromString(MQTT_HOST_IP)&&brokerIP!=IPAddress(0,0,0,0)&&MQTT_PORT>0&&p>0&&p<int(sizeof(mqttPrefix))&&a>0&&a<int(sizeof(availabilityTopic))&&b>0&&b<int(sizeof(stateTopic))&&d>0&&d<int(sizeof(commandTopic))&&strlen(MQTT_BASE_TOPIC)>0;
  snprintf(clientId,sizeof(clientId),"mistral-pr%u-%06lx",unsigned(PR_NUMBER),(unsigned long)ESP.getChipId());
  if(mqttConfigOK){
    mqtt.setServer(brokerIP,MQTT_PORT);mqtt.setClientId(clientId);mqtt.setCleanSession(true);mqtt.setKeepAlive(15);mqtt.setWill(availabilityTopic,1,true,"offline");
    if(strlen(MQTT_USERNAME))mqtt.setCredentials(MQTT_USERNAME,MQTT_PASSWORD);
    mqtt.onConnect([](bool sessionPresent){(void)sessionPresent;mqttUp=true;mqttSubscribed=false;++mqttConnects;lastSubscribe=millis();commandSubscription=mqtt.subscribe(commandTopic,1);});
    mqtt.onDisconnect([](AsyncMqttClientDisconnectReason r){mqttUp=false;mqttSubscribed=false;lastDisconnect=uint8_t(r);++mqttDisconnects;});
    mqtt.onSubscribe([](uint16_t packetId,uint8_t qos){(void)qos;if(packetId==commandSubscription)mqttSubscribed=true;});
    mqtt.onPublish([](uint16_t id){ackMailbox=id;});
    mqtt.onMessage(onMqttMessage);
  }
  web.on("/",HTTP_GET,[]{
    String html;html.reserve(480);
    html="<!doctype html><meta charset=utf-8><h2>Mistral PR";
    html+=unsigned(PR_NUMBER);
    html+=" MQTT controller 0.31.5</h2><p>Continuous bus session; gear and fan commands require fresh confirmed state.</p><p><a href='/status'>Status</a> | <a href='/raw'>Latest raw report</a> | <a href='/raw?transition=1'>Last main-view transition</a> | <a href='/trace'>Trace</a>";
    if(strlen(OTA_PASSWORD)>=16)html+=" | <a href='/update'>Wi-Fi firmware update</a>";
    html+="</p>";web.send(200,"text/html",html);
  });
  web.on("/status",HTTP_GET,status);web.on("/trace",HTTP_GET,tracePage);web.on("/raw",HTTP_GET,rawPage);web.on("/rx",HTTP_GET,rxPage);web.on("/parser",HTTP_GET,parserPage);
  if(strlen(OTA_PASSWORD)>=16)httpUpdater.setup(&web,"/update","ota",OTA_PASSWORD);
  web.onNotFound([]{if(stage==ACTIVE)++httpRequests;web.send(404,"text/plain","Not found");});web.begin();
  armedAt=millis();reason="continuous_controller";stage=ACTIVE;lastMqttService=millis()-20;
}
void loop(){
  if(stage==ACTIVE)checkControlDeadline();
  receive();partial();processIncomingCommand();
  if(stage==ACTIVE)serviceHttp();
  serviceBackground();
  if(!used&&!header&&!Serial.available())measuredYield();
  yield();
}
