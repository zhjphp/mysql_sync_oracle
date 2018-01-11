package controllers

import (
	"fmt"
	"log"
	"m2o/models"

	//"log"
)

func (c *ControllerType) M2oIndex() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	//g := c.R.FormValue("a")
	action := c.R.PostFormValue("action")
	tbName := c.R.PostFormValue("tb_name")
	pkName := c.R.PostFormValue("pk_name")
	pkValue := c.R.PostFormValue("pk_value")
	str := action + "  " + tbName + "  " + pkName + "  " + pkValue
	log.Println("do m2o : ", str)

	rest := models.Sync(action, tbName, pkName, pkValue)
	log.Println("do rest : ", rest)
	panic("")
	fmt.Fprintf(c.W, rest)

	//	fmt.Fprintf(c.W, "is do")

	//	var result string
	//	result = models.Sync(action, tbName, pkName, pkValue) + "  " + str
	//	log.Println("do result : ", result)
}
