#pragma once

class cg_sync {
public:
	
	virtual void cs_enter_rw() = 0;
	virtual void cs_enter_ro() { cs_enter_rw(); };
	virtual void cs_exit() = 0;

	virtual char *name() = 0;
};
