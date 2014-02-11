<?php  if ( ! defined('BASEPATH')) exit('No direct script access allowed');

/**
 * HandlerSocket Library for CodeIgniter
 * 
 * To use this class, please use HandlerSocket's PHP extension available at
 * http://code.google.com/p/php-handlersocket/
 * 
 * @package					CodeIgniter Library
 * @subpackage				HandlerSocket
 * @category				Database
 * @author					LuckyCat <tpovis2001@gmail.com , luckycat@mandeulgo.net>
 * @link					http://www.mandeulgo.net
 * @license					MIT License - http://www.opensource.org/licenses/mit-license.php
 * 
 */

class Hsock {
        
	protected static $CI;
	protected static $connwrite ;
	protected static $connread ;
	protected static $connindex = array() ;
	protected $index = 1;
	protected $db_name;
	protected $db_table;

	/**
	 * HandlerSocket Constructor
	 *
	 *
	 * @access        public
	 * @return        none
	 */
	function __construct() {
		$this->CI =& get_instance();
		$this->db_name = $this->CI->db->database;

		$this->connwrite = new HandlerSocket(
				$this->CI->config->item('hsock_host'),
				$this->CI->config->item('hsock_port_write')
		);
		$this->connread = new HandlerSocket(
				$this->CI->config->item('hsock_host'),
				$this->CI->config->item('hsock_port_read')
		);
	}

	private function _open($db_table, $columns = array(), $write = FALSE , $primary_key = NULL)
	{
		if($write === FALSE) 
			$type = 'connread';
		else
			$type = 'connwrite';

		$hash = $db_table;
		if(is_array($columns)) $hash .= implode('', $columns);
		else $hash .= $columns;

		$hash.= $primary_key;
		$hash = md5($hash);

		if( ($index = $this->_get_index($hash, $type) ) !== false ) return $index;

		$columns1 = implode(',', $columns);

		if( is_null($primary_key)) $primary_key = HandlerSocket::PRIMARY;

		$status = $this->$type->openIndex($this->index, $this->db_name, $db_table, $primary_key, $columns1);



		if($status === FALSE)
			$this->error_log(__METHOD__, __LINE__, $this->$type->getError());
		else 
		{
			$rindex = $this->_set_index($hash, $type);
			$this->index++;
		}
			
		return $rindex; 		
	}

	private function _get_index($hash, $type)
	{
		if( ! isset($this->connindex[$hash][$type])) return false;
		return $this->connindex[$hash][$type];
	}

	private function _set_index($hash, $type)
	{
		return $this->connindex[$hash][$type] = $this->index;
	}

	public function get($db_table, $columns = array(), $id , $op = '=' , $limit = 1 , $offset =0 , $primary_key = NULL , $update = null, $update_value = array(), $filters = array(), $in_key = - 1, $in_values = array() )
	{
		$index = $this->_open($db_table, $columns , FALSE , $primary_key );

		if( ! is_array($id))
		{
			//executeSingle( long $id, string $operate, array $criteria [, long $limit = 1, long $offset = 0, string $update = null, array $values = array(), array $filters = array(), long $in_key = -1, array $in_values = array() ] )
			$ret = $this->connread->executeSingle($index, $op, array($id), $limit, $offset , $update , $update_value , $filters , $in_key , $in_values  );
			if($ret)
			{
				if($limit == 1) return $ret[0];
				elseif($limit > 1) return $ret;
			}
			else return false;
		}

		$array = array();
		foreach($id as $row)
		{
			$query = array($index, $op, array($row), $limit , $offset);
			array_push($array, $query);
		}
		return $this->connread->executeMulti($array);
	}

	public function add($db_table, $array = array() , $primary_key = NULL)
	{
		$columns = array_keys($array);
		$value = array_values($array);

		$index = $this->_open($db_table, $columns , true , $primary_key );
		$status = $this->connwrite->executeInsert($index, $value);
		
		if($status == FALSE)	
			$this->error_log(__METHOD__, __LINE__, $this->connwrite->getError());

		return $status;
	}

	public function update($db_table, $id, $array = array(), $op = '=' , $primary_key = NULL)
	{
		$columns = array_keys($array);
		$value = array_values($array);

		$index = $this->_open($db_table, $columns ,true , $primary_key );

		$status = $this->connwrite->executeUpdate($index, $op, array($id), $value , 1, 0);
	
		if($status == FALSE)
			$this->error_log(__METHOD__, __LINE__, $this->connwrite->getError());
			
		return $status;
	}
	
	public function del($db_table, $id, $op = '=' , $primary_key = NULL)
	{
		$index = $this->_open($db_table, array(), true, $primary_key);

		$status = $this->connwrite->executeDelete($index, $op, array($id));
		
		if($status == FALSE)
			$this->error_log(__METHOD__, __LINE__,  $this->connwrite->getError());
			
		return $status;
	}
	
	private function error_log($method, $line,  $log)
	{
		if(is_array($columns)) $columns = implode(",", $columns);
		$msg ="[ERROR] $method Line: $line $db_table, $columns Msg: $log\n";
		show_error($msg);
	}
        
}

/* End of file Hsock.php */
/* Location: application/libraries/ */